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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}

import scala.collection.mutable.ArrayBuffer

@ExpressionDescription(
  usage = """
    _FUNC_(a, b) - Returns the Levenshtein distance between `a` and `b`.
  """,
  arguments = """
    Arguments:
      * a: array
      * b: array
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array('k', 'i', 't', 't', 'e', 'n'), array('k', 'i', 't', 't', 'e', 'n'))
      0
      > SELECT _FUNC_(array('k', 'i', 't', 't', 'e', 'n'), array('s', 'i', 't', 't', 'i', 'n', 'g'))
      3
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class LevenshteinDistance(left: Expression, right: Expression) extends BinaryExpression with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def checkInputDataTypes(): TypeCheckResult =
    ExpectsInputTypes.checkInputDataTypes(Seq(left, right), Seq(ArrayType, ArrayType)) match {
      case TypeCheckSuccess if DataType.equalsStructurally(left.dataType, right.dataType) =>
        TypeCheckSuccess

      case TypeCheckSuccess =>
        TypeCheckFailure(
          s"argument 2 requires ${left.dataType.simpleString} type, " +
            s"however, '${right.sql}' is of ${right.dataType.catalogString} type."
        )

      case failure =>
        failure
    }

  @transient lazy val ArrayType(elementType, _) = left.dataType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val as = input1.asInstanceOf[ArrayData].toArray[Any](elementType)
    val bs = input2.asInstanceOf[ArrayData].toArray[Any](elementType)

    val memo = ArrayBuffer.fill[Int](as.length + 1, bs.length + 1)(0)
    for (i <- 0 to as.length) {
      memo(i)(0) = i
    }
    for (j <- 0 to bs.length) {
      memo(0)(j) = j
    }
    for {
      (a, i) <- as.zipWithIndex
      (b, j) <- bs.zipWithIndex
    } {
      memo(i + 1)(j + 1) = (memo(i)(j) + (if (a == b) 0 else 1)) min (memo(i)(j + 1) + 1) min (memo(i + 1)(j) + 1)
    }
    memo(as.length)(bs.length)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): LevenshteinDistance = copy(newLeft, newRight)

  override def prettyName: String = LevenshteinDistance.prettyName
}

object LevenshteinDistance extends ExpressionCompanion[LevenshteinDistance] {
  val prettyName: String = "levenshtein_distance"

  override def apply(es: Seq[Expression]): LevenshteinDistance = LevenshteinDistance(es(0), es(1))
}
