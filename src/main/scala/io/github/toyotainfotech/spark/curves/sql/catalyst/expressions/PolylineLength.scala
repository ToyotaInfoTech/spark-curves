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
    _FUNC_(polyline) - Returns the length of `polyline`.
  """,
  arguments = """
    Arguments:
      * polyline: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(0.0d, 0.0d), array(0.1d, 0.2d), array(0.4d, -0.2d)))
      0.724
      > SELECT _FUNC_(array(array(1d, 2d, 3d), array(2d, 4d, 6d)))
      3.742
      > SELECT _FUNC_(cast(array() as array<array<double>>))
      0.0
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class PolylineLength(child: Expression) extends UnaryExpression with ExpectsInputTypes with CodegenFallback {
  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[ArrayType] = Seq(ArrayType(ArrayType(DoubleType)))

  override def nullSafeEval(input: Any): Any = {
    val as = input.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    if (as.length <= 1) return 0.0

    as.sliding(2).map(pair => dist(pair.head, pair.last)).sum
  }

  override protected def withNewChildInternal(newChild: Expression): PolylineLength = copy(newChild)

  override def prettyName: String = PolylineLength.prettyName
}

object PolylineLength extends ExpressionCompanion[PolylineLength] {
  val prettyName: String = "polyline_length"

  override def apply(es: Seq[Expression]): PolylineLength = PolylineLength(es(0))
}
