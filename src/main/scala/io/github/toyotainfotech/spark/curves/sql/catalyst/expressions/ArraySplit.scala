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
    _FUNC_(array, func) - Returns the double array split from `array` by `func`.
  """,
  arguments = """
    Arguments:
      * array: array
      * func: function
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(0, 1), array(1, 2), array(2, 4), array(3, 3), array(4, 5), array(5, 0)), (a, b) -> a[1] > b[1])
      [[0, 1], [1, 2], [2, 4]], [[3, 3], [4, 5]], [[5, 0]]]
      > SELECT _FUNC_(array(), (a, b) -> a > b)
      []
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ArraySplit(
    argument: Expression,
    function: Expression
) extends ArrayBasedSimpleHigherOrderFunction
    with CodegenFallback {
  override def dataType: DataType = ArrayType(argument.dataType)

  @transient lazy val ArrayType(elementType, containsNull) = argument.dataType

  @transient lazy val LambdaFunction(_, Seq(elementVar1: NamedLambdaVariable, elementVar2: NamedLambdaVariable), _) =
    function

  override def functionType: BooleanType = BooleanType

  override def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArraySplit =
    copy(function = f(function, (elementType, containsNull) :: (elementType, containsNull) :: Nil))

  override def nullSafeEval(inputRow: InternalRow, argumentValue: Any): Any = {
    val array = argumentValue.asInstanceOf[ArrayData]
    val buffers = ArrayBuffer[ArrayBuffer[Any]]()
    var i = 0
    if (array.numElements() > 0) {
      buffers += ArrayBuffer(array.get(i, elementType))
    }
    while (i < array.numElements() - 1) {
      elementVar1.value.set(array.get(i, elementType))
      elementVar2.value.set(array.get(i + 1, elementType))
      if (functionForEval.eval(inputRow).asInstanceOf[Boolean]) {
        buffers += ArrayBuffer(elementVar2.value.get)
      } else {
        buffers.last += elementVar2.value.get
      }
      i += 1
    }
    new GenericArrayData(buffers.map(new GenericArrayData(_)))
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ArraySplit = copy(newLeft, newRight)

  override def prettyName: String = ArraySplit.prettyName
}

object ArraySplit extends ExpressionCompanion[ArraySplit] {
  val prettyName: String = "array_split"

  def apply(es: Seq[Expression]): ArraySplit = ArraySplit(es(0), es(1))
}
