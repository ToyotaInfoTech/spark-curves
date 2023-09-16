/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package io.github.toyotainfotech.spark.curves.sql.catalyst.expressions

import io.github.toyotainfotech.spark.curves.calculations.EuclideanSpace.{dist, nearestInteriorPoint, sqdist}
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

import scala.collection.mutable.ListBuffer

@ExpressionDescription(
  usage = """
    _FUNC_(as, bs) - Returns the continuous dynamic time warping between `as` and `bs`.
      * approximate algorithm: greedy matching with the memo from as(i) to interior point between bs(j) and bs(j+1) and vice versa
      * point similarity: squared Euclidean distance
      * integral weight: sum of two path length rates
  """,
  arguments = """
    Arguments:
      * as: array<array<double>>
      * bs: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(-1d, 1d), array(0d, 1d), array(0d, 2d), array(1d, 2d), array(2d, 2d), array(3d, 2d), array(3d, 1d), array(4d, 1d)), array(array(-1d, 0d), array(0d, 0d), array(3d, 0d), array(4d, 0d)))
      2.722
      > SELECT _FUNC_(array(array(1d, 1d), array(2d, 1d), array(2d, 2d)), array(array(1d, 1d), array(2d, 1d), array(2d, 2d)))
      0.0
      > SELECT _FUNC_(array(array(1d, 1d), array(3d, 1d), array(5d, 1d), array(7d, 1d)), array(array(0d, 1d), array(2d, 0d), array(4d, 0d), array(6d, 0d), array(7d, 0d)))
      0.881
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ContinuousDynamicTimeWarping(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes
    with CodegenFallback {
  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[ArrayType] = Seq(ArrayType(ArrayType(DoubleType)), ArrayType(ArrayType(DoubleType)))

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val as = input1.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    val bs = input2.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    assert(as.length >= 2, s"The argument `as` length of $prettyName must be greater than or equal to 2.")
    assert(bs.length >= 2, s"The argument `bs` length of $prettyName must be greater than or equal to 2.")
    assert(as.forall(_.length == as.head.length), s"The argument `as` of $prettyName must be not jagged array.")
    assert(bs.forall(_.length == bs.head.length), s"The argument `as` of $prettyName must be not jagged array.")
    assert(
      as.head.length == bs.head.length,
      s"The argument `as` and 'bs' of $prettyName must be in the same dimension."
    )

    ContinuousDynamicTimeWarping.similarity(as, bs)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ContinuousDynamicTimeWarping = copy(newLeft, newRight)

  override def prettyName: String = ContinuousDynamicTimeWarping.prettyName
}

object ContinuousDynamicTimeWarping extends ExpressionCompanion[ContinuousDynamicTimeWarping] {
  val prettyName: String = "continuous_dynamic_time_warping"

  override def apply(es: Seq[Expression]): ContinuousDynamicTimeWarping = ContinuousDynamicTimeWarping(es(0), es(1))

  private def calc(
      as: Array[Array[Double]],
      bs: Array[Array[Double]]
  ): Memo = {
    val memosAtoB = Array.fill[MemoAtoB](as.length, bs.length)(null) // i => j-1 ~ j
    val memosBtoA = Array.fill[MemoBtoA](as.length, bs.length)(null) // i-1 ~ i <= j

    for (i <- as.indices) {
      for (j <- bs.indices) {
        memosAtoB(i)(j) = (i, j) match {
          case (0, 0) =>
            MemoNil().addAtoB(as.head, bs.head)
          case (_, 0) =>
            memosAtoB(i - 1)(0).addAtoB(as(i), bs.head)
          case (0, _) =>
            memosBtoA(0)(j - 1).addAtoB(as.head, nearestInteriorPoint(as.head)(bs(j - 1), bs(j)))
          case (_, _) =>
            orderingAtoB.min(
              memosAtoB(i - 1)(j).addAtoB(as(i), nearestInteriorPoint(as(i))(memosAtoB(i - 1)(j).b, bs(j))),
              memosBtoA(i)(j - 1).addAtoB(as(i), nearestInteriorPoint(as(i))(bs(j - 1), bs(j)))
            )
        }
        memosBtoA(i)(j) = (i, j) match {
          case (0, 0) =>
            MemoNil().addBtoA(bs.head, as.head)
          case (0, _) =>
            memosBtoA(0)(j - 1).addBtoA(bs(j), as.head)
          case (_, 0) =>
            memosAtoB(i - 1)(0).addBtoA(bs(j), nearestInteriorPoint(bs(j))(as(i - 1), as(i)))
          case (_, _) =>
            orderingBtoA.min(
              memosBtoA(i)(j - 1).addBtoA(bs(j), nearestInteriorPoint(bs(j))(memosBtoA(i)(j - 1).a, as(i))),
              memosAtoB(i - 1)(j).addBtoA(bs(j), nearestInteriorPoint(bs(j))(as(i - 1), as(i)))
            )
        }
      }
    }

    ordering.min(
      memosAtoB.last.last.addBtoA(bs.last, as.last),
      memosBtoA.last.last.addAtoB(as.last, bs.last)
    )
  }

  def similarity(as: Array[Array[Double]], bs: Array[Array[Double]]): Double = {
    val length =
      as.sliding(2).map(pair => dist(pair.head, pair.last)).sum +
        bs.sliding(2).map(pair => dist(pair.head, pair.last)).sum
    if (length > 0) {
      calc(as, bs).sqdistIntegral / length
    } else {
      sqdist(as.head, bs.head)
    }
  }

  def matchAtoB(as: Array[Array[Double]], bs: Array[Array[Double]]): Array[Array[Double]] = {
    val buffer = ListBuffer[Array[Double]]()
    var memo: Option[Memo] = Some(calc(as, bs))
    while (memo.nonEmpty) {
      memo = memo.get match {
        case MemoNil() =>
          None
        case MemoAtoB(_, _, b, prev) =>
          buffer.prepend(b)
          Some(prev)
        case MemoBtoA(_, _, _, prev) =>
          Some(prev)
      }
    }
    val result = buffer.toArray
    result(0) = bs.head
    result(result.length - 1) = bs.last
    result
  }

  private sealed trait Memo {
    val sqdistIntegral: Double
    def addAtoB(a: Array[Double], b: Array[Double]): MemoAtoB
    def addBtoA(b: Array[Double], a: Array[Double]): MemoBtoA
  }
  private case class MemoNil() extends Memo {
    val sqdistIntegral: Double = 0.0

    def addAtoB(a: Array[Double], b: Array[Double]): MemoAtoB =
      MemoAtoB(0.0, a, b, this)

    def addBtoA(b: Array[Double], a: Array[Double]): MemoBtoA =
      MemoBtoA(0.0, b, a, this)
  }
  private sealed trait MemoCons extends Memo {
    val a: Array[Double]
    val b: Array[Double]

    def addAtoB(a: Array[Double], b: Array[Double]): MemoAtoB = {
      val sd = sqdistMean(this.a, a, this.b, b) * (dist(this.a, a) + dist(this.b, b))
      MemoAtoB(sqdistIntegral + sd, a, b, this)
    }

    def addBtoA(b: Array[Double], a: Array[Double]): MemoBtoA = {
      val sd = sqdistMean(this.a, a, this.b, b) * (dist(this.a, a) + dist(this.b, b))
      MemoBtoA(sqdistIntegral + sd, b, a, this)
    }

    // \int_{0}^{1} \mathrm{sqdist}(a_0 t + a_1 (1 - t), b_0 t + b_1 (1 - t)) dt
    private def sqdistMean(a0: Array[Double], a1: Array[Double], b0: Array[Double], b1: Array[Double]): Double = {
      var result = 0.0
      var i = 0
      while (i < a.length) {
        val d0 = a0(i) - b0(i)
        val d1 = a1(i) - b1(i)
        result += (d0 * d0 + d0 * d1 + d1 * d1) / 3
        i += 1
      }
      result
    }
  }
  private case class MemoAtoB(sqdistIntegral: Double, a: Array[Double], b: Array[Double], prev: Memo) extends MemoCons
  private case class MemoBtoA(sqdistIntegral: Double, b: Array[Double], a: Array[Double], prev: Memo) extends MemoCons

  private val ordering = Ordering.by((_: Memo).sqdistIntegral)
  private val orderingAtoB = Ordering.by((_: MemoAtoB).sqdistIntegral)
  private val orderingBtoA = Ordering.by((_: MemoBtoA).sqdistIntegral)
}

@ExpressionDescription(
  usage = """
    _FUNC_(as, bs) - Returns the matching points form `as` to `bs` in calculation of continuous dynamic time warping.
      * approximate algorithm: greedy matching with the memo from as(i) to interior point between bs(j) and bs(j+1) and vice versa
      * point similarity: squared Euclidean distance
      * integral weight: sum of two path length rates
  """,
  arguments = """
    Arguments:
      * as: array<array<double>>
      * bs: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(-1d, 1d), array(0d, 1d), array(0d, 2d), array(1d, 2d), array(2d, 2d), array(3d, 2d), array(3d, 1d), array(4d, 1d)), array(array(-1d, 0d), array(0d, 0d), array(3d, 0d), array(4d, 0d)))
      [[-1.0, 0.0], [0.0, 0.0], [0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0], [3.0, 0.0], [4.0, 0.0]]
      > SELECT _FUNC_(array(array(-1d, 0d), array(0d, 0d), array(3d, 0d), array(4d, 0d)), array(array(-1d, 1d), array(0d, 1d), array(0d, 2d), array(1d, 2d), array(2d, 2d), array(3d, 2d), array(3d, 1d), array(4d, 1d)))
      [[-1.0, 1.0], [0.0, 1.0], [3.0, 1.0], [4.0, 1.0]]
      > SELECT _FUNC_(array(array(1d, 1d), array(2d, 1d), array(3d, 1d), array(4d, 1d)), array(array(1d, 0d), array(0d, 0d), array(0.5d, 0d), array(2d, 0d), array(2d, -1d), array(2d, 0d), array(3.5d, 0d), array(3.5d, -1d), array(3.5d, 0d), array(4.5d, 0d), array(5d, 0d), array(2d, 0d)))
      [[1.0, 0.0], [2.0, 0.0], [3.0, 0.0], [2.0, 0.0]]
      > SELECT _FUNC_(array(array(1d, 0d), array(0d, 0d), array(0.5d, 0d), array(2d, 0d), array(2d, -1d), array(2d, 0d), array(3.5d, 0d), array(3.5d, -1d), array(3.5d, 0d), array(4.5d, 0d), array(5d, 0d), array(2d, 0d)), array(array(1d, 1d), array(2d, 1d), array(3d, 1d), array(4d, 1d)))
      [[1.0, 1.0], [1.0, 1.0], [1.0, 1.0], [2.0, 1.0], [2.0, 1.0], [2.0, 1.0], [3.5, 1.0], [3.5, 1.0], [3.5, 1.0], [4.0, 1.0], [4.0, 1.0], [4.0, 1.0]]
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ContinuousDynamicTimeWarpingMatch(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes
    with CodegenFallback {
  override def dataType: DataType = ArrayType(ArrayType(DoubleType))

  override def inputTypes: Seq[ArrayType] = Seq(ArrayType(ArrayType(DoubleType)), ArrayType(ArrayType(DoubleType)))

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val as = input1.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    val bs = input2.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    assert(as.length >= 2, s"The argument `as` length of $prettyName must be greater than or equal to 2.")
    assert(bs.length >= 2, s"The argument `bs` length of $prettyName must be greater than or equal to 2.")
    assert(as.forall(_.length == as.head.length), s"The argument `as` of $prettyName must be not jagged array.")
    assert(bs.forall(_.length == bs.head.length), s"The argument `as` of $prettyName must be not jagged array.")
    assert(
      as.head.length == bs.head.length,
      s"The argument `as` and 'bs' of $prettyName must be in the same dimension."
    )

    new GenericArrayData(ContinuousDynamicTimeWarping.matchAtoB(as, bs).map(new GenericArrayData(_)))
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ContinuousDynamicTimeWarpingMatch = copy(newLeft, newRight)

  override def prettyName: String = ContinuousDynamicTimeWarpingMatch.prettyName
}

object ContinuousDynamicTimeWarpingMatch extends ExpressionCompanion[ContinuousDynamicTimeWarpingMatch] {
  val prettyName: String = "continuous_dynamic_time_warping_match"

  override def apply(es: Seq[Expression]): ContinuousDynamicTimeWarpingMatch =
    ContinuousDynamicTimeWarpingMatch(es(0), es(1))
}
