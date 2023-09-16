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
import org.apache.spark.sql.types.*

import scala.collection.mutable.ArrayBuffer

@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the array removed consecutive duplicate values from `array`.
  """,
  arguments = """
    Arguments:
      * array: array
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('a', 'a', 'a', 'b', 'c', 'c', 'a'))
      ["a", "b", "c", "a"]
      > SELECT _FUNC_(array())
      []
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ArrayConsecutiveDistinct(child: Expression)
    extends UnaryExpression
    with ExpectsInputTypes
    with CodegenFallback {
  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[ArrayType.type] = Seq(ArrayType)

  @transient lazy val ArrayType(elementType, _) = child.dataType

  override def nullSafeEval(input: Any): Any = {
    val array = input.asInstanceOf[ArrayData]
    if (array.numElements() == 0) return input

    var latest = array.get(0, elementType)
    val buffer = ArrayBuffer[Any](latest)
    var i = 1
    while (i < array.numElements()) {
      val current = array.get(i, elementType)
      if (current != latest) {
        latest = current
        buffer += current
      }
      i += 1
    }
    new GenericArrayData(buffer)
  }

  override protected def withNewChildInternal(newChild: Expression): ArrayConsecutiveDistinct = copy(newChild)

  override def prettyName: String = ArrayConsecutiveDistinct.prettyName
}

object ArrayConsecutiveDistinct extends ExpressionCompanion[ArrayConsecutiveDistinct] {
  val prettyName: String = "array_consecutive_distinct"

  override def apply(es: Seq[Expression]): ArrayConsecutiveDistinct = ArrayConsecutiveDistinct(es(0))
}
