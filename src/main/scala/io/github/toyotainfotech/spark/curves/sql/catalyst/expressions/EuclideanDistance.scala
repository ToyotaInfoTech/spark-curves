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
    _FUNC_(a, b) - Returns the Euclidean distance between `a` and `b`.
  """,
  arguments = """
    Arguments:
      * a: array<double>
      * b: array<double>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(0.0d, 0.0d), array(0.1d, 0.1d))
      0.141
      > SELECT _FUNC_(array(1d, 2d, 3d), array(2d, 4d, 6d))
      3.742
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class EuclideanDistance(left: Expression, right: Expression)
    extends BinaryExpression
    with ExpectsInputTypes
    with CodegenFallback {
  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[ArrayType] = Seq(ArrayType(DoubleType), ArrayType(DoubleType))

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val as = input1.asInstanceOf[ArrayData].toDoubleArray()
    val bs = input2.asInstanceOf[ArrayData].toDoubleArray()
    dist(as, bs)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): EuclideanDistance = copy(newLeft, newRight)

  override def prettyName: String = EuclideanDistance.prettyName
}

object EuclideanDistance extends ExpressionCompanion[EuclideanDistance] {
  val prettyName: String = "euclidean_distance"

  override def apply(es: Seq[Expression]): EuclideanDistance = EuclideanDistance(es(0), es(1))
}
