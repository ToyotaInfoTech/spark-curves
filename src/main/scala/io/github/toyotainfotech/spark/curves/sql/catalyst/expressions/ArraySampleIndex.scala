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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.*

@ExpressionDescription(
  usage = """
    _FUNC_(array, n) - Returns the `n`-sized polyline sampled indices of `array`.
  """,
  arguments = """
    Arguments:
      * array: array
      * func: function
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(0, 1, 2, 3, 4, 5, 6, 7), 5)
      [0, 2, 4, 5, 7]
      > SELECT _FUNC_(array(0, 1, 2), 7)
      [0, 0, 1, 1, 1, 2, 2]
      > SELECT _FUNC_(array(0, 1, 2), 1)
      [0]
      > SELECT _FUNC_(array(0, 1, 2), 0)
      []
      > SELECT _FUNC_(array(), 7)
      []
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ArraySampleIndex(left: Expression, right: Expression) extends BinaryExpression with CodegenFallback {
  override def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    ExpectsInputTypes.checkInputDataTypes(Seq(left, right), Seq(ArrayType, IntegerType))

  @transient lazy val ArrayType(elementType, _) = left.dataType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val as = input1.asInstanceOf[ArrayData]
    val n = input2.asInstanceOf[Int]
    if (as.numElements() == 0) return input1
    if (n == 0) return new GenericArrayData(Array())
    if (n == 1) return new GenericArrayData(Array(as.get(0, elementType)))

    val result = (0 until n).map(i => as.get((i.toDouble * (as.numElements() - 1) / (n - 1)).round.toInt, elementType))
    new GenericArrayData(result)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ArraySampleIndex = copy(newLeft, newRight)

  override def prettyName: String = ArraySampleIndex.prettyName
}

object ArraySampleIndex extends ExpressionCompanion[ArraySampleIndex] {
  val prettyName: String = "array_sample_index"

  override def apply(es: Seq[Expression]): ArraySampleIndex = ArraySampleIndex(es(0), es(1))
}
