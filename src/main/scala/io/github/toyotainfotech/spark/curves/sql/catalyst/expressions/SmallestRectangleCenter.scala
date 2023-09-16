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

import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

@ExpressionDescription(
  usage = """
    _FUNC_(points) - Returns the center of smallest rectangle enclosing `points`.
  """,
  arguments = """
    Arguments:
      * points: array<array<double>>
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(0d, 0d), array(0d, 2d), array(2d, 2d), array(2d, 0d)))
      [1.0, 1.0]
      > SELECT _FUNC_(array(array(3d, -2d), array(3d, 5d), array(-4d, 1d), array(-4d, -2d), array(5d, 1d)))
      [0.5, 1.5]
      > SELECT _FUNC_(cast(array() as array<array<double>>))
      []
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class SmallestRectangleCenter(child: Expression)
    extends UnaryExpression
    with ExpectsInputTypes
    with CodegenFallback {
  override def dataType: DataType = ArrayType(DoubleType)

  override def inputTypes: Seq[ArrayType] = Seq(ArrayType(ArrayType(DoubleType)))

  override def nullSafeEval(input: Any): Any = {
    val as = input.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    val result = as.transpose.map(ps => (ps.max + ps.min) / 2)
    new GenericArrayData(result)
  }

  override protected def withNewChildInternal(newChild: Expression): SmallestRectangleCenter = copy(newChild)

  override def prettyName: String = SmallestRectangleCenter.prettyName
}

object SmallestRectangleCenter extends ExpressionCompanion[SmallestRectangleCenter] {
  val prettyName: String = "smallest_rectangle_center"

  override def apply(es: Seq[Expression]): SmallestRectangleCenter = SmallestRectangleCenter(es(0))
}
