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

import io.github.toyotainfotech.spark.curves.calculations.EuclideanSpace.{dist, interiorPoint}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.*

import scala.collection.mutable

@ExpressionDescription(
  usage = """
    _FUNC_(polyline, n) - Returns the `n`-sized polyline sampled equidistantly from `polyline`.
  """,
  arguments = """
    Arguments:
      * polyline: array<array<double>>
      * n: int
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(array(0d, 0d), array(0d, 1d), array(3d, 1d), array(3d, -9d)), 5)
      [[0.0, 0.0], [2.5, 1.0], [3.0, -2.0], [3.0, -5.5], [3.0, -9.0]]
      > SELECT _FUNC_(array(array(1d, 2d), array(1d, 2d)), 4)
      [[1.0, 2.0], [1.0, 2.0], [1.0, 2.0], [1.0, 2.0]]
      > SELECT _FUNC_(array(array(1d, 2d), array(3d, 4d)), 1)
      [[1.0, 2.0]]
      > SELECT _FUNC_(array(array(1d, 2d), array(3d, 4d)), 0)
      []
      > SELECT _FUNC_(array(array(1d, 2d)), 3)
      [[1.0, 2.0], [1.0, 2.0], [1.0, 2.0]]
      > SELECT _FUNC_(cast(array() as array<array<double>>), 3)
      []
  """,
  source = "io.github.toyotainfotech.spark.curves"
)
case class PolylineSampleEquidistant(left: Expression, right: Expression)
    extends BinaryExpression
    with CodegenFallback {
  override def dataType: DataType = ArrayType(ArrayType(DoubleType))

  override def checkInputDataTypes(): TypeCheckResult =
    ExpectsInputTypes.checkInputDataTypes(children, Seq(ArrayType(ArrayType(DoubleType)), IntegerType))

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val as = input1.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    val n = input2.asInstanceOf[Int]
    if (as.length == 0) return input1
    if (as.length == 1) return new GenericArrayData(Array.fill(n)(new GenericArrayData(as.head)))
    if (n == 0) return new GenericArrayData(Array())
    if (n == 1) return new GenericArrayData(Array(new GenericArrayData(as.head)))

    val lines = as.sliding(2).map(pair => Line(pair.head, pair.last, dist(pair.head, pair.last))).toBuffer
    val result = mutable.ListBuffer(as.head)
    val interval = lines.map(_.distance).sum / (n - 1)
    var remains = interval
    while (result.size < n - 1 && lines.nonEmpty) {
      val line = lines.head
      if (remains <= line.distance) {
        val p = interiorPoint(line.source, line.destination, if (line.distance > 0) remains / line.distance else 0)
        result += p
        lines(0) = Line(p, line.destination, line.distance - remains)
        remains = interval
      } else {
        lines.remove(0)
        remains -= line.distance
      }
    }
    result += as.last
    new GenericArrayData(result.map(new GenericArrayData(_)))
  }

  private case class Line(source: Array[Double], destination: Array[Double], distance: Double)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): PolylineSampleEquidistant = copy(newLeft, newRight)

  override def prettyName: String = PolylineSampleEquidistant.prettyName
}

object PolylineSampleEquidistant extends ExpressionCompanion[PolylineSampleEquidistant] {
  val prettyName: String = "polyline_sample_equidistant"

  override def apply(es: Seq[Expression]): PolylineSampleEquidistant = PolylineSampleEquidistant(es(0), es(1))
}
