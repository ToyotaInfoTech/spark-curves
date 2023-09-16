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

import io.github.toyotainfotech.spark.curves.calculations.EuclideanSpace.sqdist
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

@ExpressionDescription(
  usage = """
    _FUNC_(as, bs) - Returns the discrete Hausdorff distance between `as` and `bs`.
  """,
  arguments = """
    Arguments:
      * as: array<array<double>>
      * bs: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(1d, 0d), array(2d, 0d), array(3d, 0d)), array(array(1d, 1d), array(2d, 2d), array(3d, 3d), array(4d, 4d)))
      4.123
      > SELECT _FUNC_(array(array(-0.1d, -0.1d), array(0.3d, -0.1d), array(0.3d, 0.4d), array(-0.1d, 0.4d)), array(array(0d, 0d), array(0.1d, 0d), array(0.1d, 0.1d), array(0d, 0.1d)))
      0.361
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class DiscreteHausdorffDistance(left: Expression, right: Expression)
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

    math.sqrt(
      math.max(
        as.map(a => bs.map(b => sqdist(a, b)).min).max,
        bs.map(b => as.map(a => sqdist(a, b)).min).max
      )
    )
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): DiscreteHausdorffDistance = copy(newLeft, newRight)

  override def prettyName: String = DiscreteHausdorffDistance.prettyName
}

object DiscreteHausdorffDistance extends ExpressionCompanion[DiscreteHausdorffDistance] {
  val prettyName: String = "discrete_hausdorff_distance"

  override def apply(es: Seq[Expression]): DiscreteHausdorffDistance = DiscreteHausdorffDistance(es(0), es(1))
}
