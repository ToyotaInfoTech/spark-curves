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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.types.*

@ExpressionDescription(
  usage = """
    _FUNC_(array, func) - Returns the array sorted from `array` by `func`.
  """,
  arguments = """
    Arguments:
      * array: array
      * func: function
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(5, -3, -1, 2 ,-4), a -> abs(a))
      [-1, 2, -3, -4, 5]
      > SELECT _FUNC_(array(array('C', 'a'), array('B', 'c'), array('A', 'b')), a -> a[1])
      [["C", "a"], ["A", "b"], ["B", "c"]]
      > SELECT _FUNC_(array(), a -> a > 0)
      []
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class ArraySortBy(
    argument: Expression,
    function: Expression
) extends ArrayBasedSimpleHigherOrderFunction
    with CodegenFallback {
  override def dataType: DataType = argument.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    checkArgumentDataTypes() match {
      case TypeCheckSuccess =>
        if (RowOrdering.isOrderable(function.dataType)) {
          TypeCheckSuccess
        } else {
          TypeCheckFailure("Return type of the given function has to be Orderable.")
        }
      case failure => failure
    }

  @transient lazy val ArrayType(elementType, containsNull) = argument.dataType

  @transient lazy val LambdaFunction(_, Seq(elementVar: NamedLambdaVariable), _) = function

  override def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): ArraySortBy =
    copy(function = f(function, (elementType, containsNull) :: Nil))

  override def nullSafeEval(inputRow: InternalRow, argumentValue: Any): Any = {
    val array = argumentValue.asInstanceOf[ArrayData]
    val result = array
      .toArray[Any](elementType)
      .sortBy(a => {
        elementVar.value.set(a)
        functionForEval.eval(inputRow)
      })(TypeUtils.getInterpretedOrdering(function.dataType))
    new GenericArrayData(result)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): ArraySortBy = copy(newLeft, newRight)

  override def prettyName: String = ArraySortBy.prettyName
}

object ArraySortBy extends ExpressionCompanion[ArraySortBy] {
  val prettyName: String = "array_sort_by"

  def apply(es: Seq[Expression]): ArraySortBy = ArraySortBy(es(0), es(1))
}
