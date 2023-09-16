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

import io.github.toyotainfotech.spark.curves.calculations.EuclideanSpace.dist
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

@ExpressionDescription(
  usage = """
    _FUNC_(as, bs) - Returns the discrete dynamic time warping between `as` and `bs`.
  """,
  arguments = """
    Arguments:
      * as: array<array<double>>
      * bs: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(0d, 0d), array(2d, 0d), array(6d, 0d)), array(array(0d, 1d), array(2d, 1d), array(3d, 1d), array(6d, 1d)))
      4.4142
      > SELECT _FUNC_(array(array(1d, 1d), array(2d, 1d), array(2d, 2d)), array(array(1d, 1d), array(2d, 1d), array(2d, 2d), array(2d, 2d)))
      0.0
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class DiscreteDynamicTimeWarping(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes
    with CodegenFallback {
  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[ArrayType] = Seq(ArrayType(ArrayType(DoubleType)), ArrayType(ArrayType(DoubleType)))

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val as = input1.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    val bs = input2.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    assert(as.length >= 1, s"The argument `as` of $prettyName must be not empty.")
    assert(bs.length >= 1, s"The argument `bs` of $prettyName must be not empty.")
    assert(as.forall(_.length == as.head.length), s"The argument `as` of $prettyName must be not jagged array.")
    assert(bs.forall(_.length == bs.head.length), s"The argument `as` of $prettyName must be not jagged array.")
    assert(
      as.head.length == bs.head.length,
      s"The argument `as` and 'bs' of $prettyName must be in the same dimension."
    )

    val memo = Array.fill(as.length, bs.length)(Double.NaN)
    for (i <- as.indices) {
      for (j <- bs.indices) {
        val d = dist(as(i), bs(j))
        memo(i)(j) = (i, j) match {
          case (0, 0) =>
            d
          case (_, 0) =>
            d + memo(i - 1)(0)
          case (0, _) =>
            d + memo(0)(j - 1)
          case (_, _) =>
            d + (memo(i - 1)(j) min memo(i)(j - 1) min memo(i - 1)(j - 1))
        }
      }
    }
    memo.last.last
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): DiscreteDynamicTimeWarping = copy(newLeft, newRight)

  override def prettyName: String = DiscreteDynamicTimeWarping.prettyName
}

object DiscreteDynamicTimeWarping extends ExpressionCompanion[DiscreteDynamicTimeWarping] {
  val prettyName: String = "discrete_dynamic_time_warping"

  override def apply(es: Seq[Expression]): DiscreteDynamicTimeWarping = DiscreteDynamicTimeWarping(es(0), es(1))
}
