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
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

import scala.collection.mutable
import scala.util.Random

@ExpressionDescription(
  usage = """
    _FUNC_(points) - Returns the center of smallest center enclosing `points`.
  """,
  arguments = """
    Arguments:
      * points: array<2D array<double>>
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
case class SmallestCircleCenter(child: Expression, tolerance: Double = 0.001)
    extends UnaryExpression
    with ExpectsInputTypes
    with CodegenFallback {
  override def dataType: DataType = ArrayType(DoubleType)

  override def inputTypes: Seq[ArrayType] = Seq(ArrayType(ArrayType(DoubleType)))

  // cf. http://dopal.cs.uec.ac.jp/okamotoy/lect/2017/geomcover/
  // cf. https://qiita.com/tatesuke/items/59133758ec25146766c3
  override def nullSafeEval(input: Any): Any = {
    val as = input.asInstanceOf[ArrayData].toArray[ArrayData](ArrayType(DoubleType)).map(_.toDoubleArray())
    assert(as.forall(_.length == 2), s"The argument `points` of $prettyName must be 2D.")
    if (as.length == 0) return new GenericArrayData(Array())

    val random = new Random()

    val stack = mutable.Stack[(Vector[Array[Double]], List[Array[Double]], Option[Array[Double]])]()
    stack.push((as.toSet.toVector, List(), None))
    var returnValue = Option.empty[(Array[Double], Double)]

    while (stack.nonEmpty) {
      stack.pop() match {
        case (points, Seq(a0, a1, a2), None) =>
          val a12_0 = a2(0) - a1(0)
          val a12_1 = a2(1) - a1(1)
          val a20_0 = a0(0) - a2(0)
          val a20_1 = a0(1) - a2(1)
          val a01_0 = a1(0) - a0(0)
          val a01_1 = a1(1) - a0(1)
          val e0 = a12_0 * a12_0 + a12_1 * a12_1
          val e1 = a20_0 * a20_0 + a20_1 * a20_1
          val e2 = a01_0 * a01_0 + a01_1 * a01_1
          val w0 = e0 * (e1 + e2 - e0)
          val w1 = e1 * (e2 + e0 - e1)
          val w2 = e2 * (e0 + e1 - e2)
          val w = w0 + w1 + w2
          val circle = if (w != 0) {
            val c_0 = (a0(0) * w0 + a1(0) * w1 + a2(0) * w2) / w
            val c_1 = (a0(1) * w0 + a1(1) * w1 + a2(1) * w2) / w
            val ca_0 = a0(0) - c_0
            val ca_1 = a0(1) - c_1
            (Array(c_0, c_1), math.sqrt(ca_0 * ca_0 + ca_1 * ca_1))
          } else {
            (a0, 0.0)
          }
          if (points.forall(inside(circle, _))) {
            returnValue = Some(circle)
          } else {
            returnValue = None
          }

        case (Seq(), Seq(a0, a1), None) =>
          val a01_0 = a1(0) - a0(0)
          val a01_1 = a1(1) - a0(1)
          returnValue = Some(
            (Array((a0(0) + a1(0)) / 2, (a0(1) + a1(1)) / 2), math.sqrt(a01_0 * a01_0 + a01_1 * a01_1) / 2)
          )

        case (Seq(), Seq(a0), None) =>
          returnValue = Some((a0, 0))

        case (Seq(), _, None) =>
          returnValue = None

        case (points, boundaryPoints, None) =>
          val i = random.nextInt(points.size)
          val newPoints = points.take(i) ++ points.drop(i + 1)
          stack.push((newPoints, boundaryPoints, Some(points(i))))
          stack.push((newPoints, boundaryPoints, None))

        case (points, boundaryPoints, Some(selectedPoint)) =>
          returnValue match {
            case Some(circle) if inside(circle, selectedPoint) =>

            case _ =>
              stack.push((points, selectedPoint +: boundaryPoints, None))
          }
      }
    }

    val result = returnValue.map(_._1).getOrElse(Array(0.0, 0.0))
    new GenericArrayData(result)
  }

  private def inside(circle: (Array[Double], Double), p: Array[Double]): Boolean =
    dist(circle._1, p) <= (circle._2 * (1 + tolerance))

  override protected def withNewChildInternal(newChild: Expression): SmallestCircleCenter = copy(newChild)

  override def prettyName: String = SmallestCircleCenter.prettyName
}

object SmallestCircleCenter extends ExpressionCompanion[SmallestCircleCenter] {
  val prettyName: String = "smallest_circle_center"

  override def apply(es: Seq[Expression]): SmallestCircleCenter = SmallestCircleCenter(es(0))
}
