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

import io.github.toyotainfotech.spark.curves.calculations.EuclideanSpace.{nearestInteriorPoint, sqdist}
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

@ExpressionDescription(
  usage = """
    _FUNC_(as, bs) - Returns the continuous Hausdorff distance between `as` and `bs`.
  """,
  arguments = """
    Arguments:
      * as: array<array<double>>
      * bs: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(1d, 0d), array(2d, 0d), array(3d, 0d), array(4d, 0d), array(5d, 0d), array(3d, 0d)), array(array(4d, 0d), array(3d, 2d), array(1d, 1d)))
      2.0
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ContinuousHausdorffDistance(left: Expression, right: Expression)
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

    math.sqrt(
      math.max(
        as.map(a => bs.sliding(2).map(pair => sqdist(a, nearestInteriorPoint(a)(pair.head, pair.last))).min).max,
        bs.map(b => as.sliding(2).map(pair => sqdist(b, nearestInteriorPoint(b)(pair.head, pair.last))).min).max
      )
    )
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ContinuousHausdorffDistance = copy(newLeft, newRight)

  override def prettyName: String = ContinuousHausdorffDistance.prettyName
}

object ContinuousHausdorffDistance extends ExpressionCompanion[ContinuousHausdorffDistance] {
  val prettyName: String = "continuous_hausdorff_distance"

  override def apply(es: Seq[Expression]): ContinuousHausdorffDistance = ContinuousHausdorffDistance(es(0), es(1))
}

@ExpressionDescription(
  usage = """
    _FUNC_(as, bs) - Returns the matching points form `as` to `bs` in calculation of continuous Hausdorff distance.
  """,
  arguments = """
    Arguments:
      * as: array<array<double>>
      * bs: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(1d, 0d), array(2d, 0d), array(3d, 0d), array(4d, 0d), array(5d, 0d), array(3d, 0d)), array(array(4d, 0d), array(3d, 2d), array(1d, 1d)))
      [[1.0, 1.0], [1.4, 1.2], [3.8, 0.4], [4.0, 0.0], [4.0, 0.0], [3.8, 0.4]]
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ContinuousHausdorffDistanceMatch(left: Expression, right: Expression)
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

    val result = as.map(a =>
      bs.sliding(2)
        .map(pair => {
          val p = nearestInteriorPoint(a)(pair.head, pair.last)
          (p, sqdist(a, p))
        })
        .minBy(_._2)
        ._1
    )

    new GenericArrayData(result.map(new GenericArrayData(_)))
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ContinuousHausdorffDistanceMatch = copy(newLeft, newRight)

  override def prettyName: String = ContinuousHausdorffDistanceMatch.prettyName
}

object ContinuousHausdorffDistanceMatch extends ExpressionCompanion[ContinuousHausdorffDistanceMatch] {
  val prettyName = "continuous_hausdorff_distance_match"

  override def apply(es: Seq[Expression]): ContinuousHausdorffDistanceMatch =
    ContinuousHausdorffDistanceMatch(es(0), es(1))
}
