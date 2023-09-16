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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.*

import scala.collection.mutable.ArrayBuffer

@ExpressionDescription(
  usage = """
    _FUNC_(array, func) - Returns the double array split from `array` filtered by `func`. Sub-arrays containing first or last element are excluded from them.
  """,
  arguments = """
    Arguments:
      * array: array
      * func: function
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, -3, -4, 5, 6, 7, -8, 9), a -> a > 0)
      [[5, 6, 7]]
      > SELECT _FUNC_(array(-1, -2, -3, 4, 5, -6, 7, 8, -9), a -> a > 0)
      [[4, 5], [7, 8]]
      > SELECT _FUNC_(array(), a -> a > 0)
      []
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ArrayFilterSplitWithoutEnds(
    argument: Expression,
    function: Expression
) extends ArrayBasedSimpleHigherOrderFunction
    with CodegenFallback {
  override def dataType: DataType = ArrayType(argument.dataType)

  @transient lazy val ArrayType(elementType, containsNull) = argument.dataType

  @transient lazy val LambdaFunction(_, Seq(elementVar: NamedLambdaVariable), _) = function

  override def functionType: BooleanType = BooleanType

  override def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArrayFilterSplitWithoutEnds =
    copy(function = f(function, (elementType, containsNull) :: Nil))

  override def nullSafeEval(inputRow: InternalRow, argumentValue: Any): Any = {
    val array = argumentValue.asInstanceOf[ArrayData]
    var buffer = ArrayBuffer[Any]()
    val buffers = ArrayBuffer[GenericArrayData]()
    var start = true
    var i = 0
    while (i < array.numElements()) {
      elementVar.value.set(array.get(i, elementType))
      if (functionForEval.eval(inputRow).asInstanceOf[Boolean]) {
        buffer += elementVar.value.get
      } else {
        if (buffer.nonEmpty) {
          if (!start) {
            buffers += new GenericArrayData(buffer)
          }
          buffer = ArrayBuffer[Any]()
        }
        start = false
      }
      i += 1
    }
    new GenericArrayData(buffers)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ArrayFilterSplitWithoutEnds = copy(newLeft, newRight)

  override def prettyName: String = ArrayFilterSplitWithoutEnds.prettyName
}

object ArrayFilterSplitWithoutEnds extends ExpressionCompanion[ArrayFilterSplitWithoutEnds] {
  val prettyName: String = "array_filter_split_without_ends"

  def apply(es: Seq[Expression]): ArrayFilterSplitWithoutEnds = ArrayFilterSplitWithoutEnds(es(0), es(1))
}
