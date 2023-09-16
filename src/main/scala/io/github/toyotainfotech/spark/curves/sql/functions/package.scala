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
package io.github.toyotainfotech.spark.curves.sql

import io.github.toyotainfotech.spark.curves.sql.catalyst.expressions.*
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.*
import org.apache.spark.sql.functions.lit

import scala.reflect.ClassTag

package object functions {
  def array_consecutive_distinct(a: Column): Column = new Column(
    ArrayConsecutiveDistinct(a.expr)
  )

  def array_filter_split(a: Column, f: Column => Column) = new Column(
    ArrayFilterSplit(a.expr, createLambda(f))
  )

  def array_filter_split_without_ends(a: Column, f: Column => Column) = new Column(
    ArrayFilterSplitWithoutEnds(a.expr, createLambda(f))
  )

  def array_sample_index(a: Column, b: Column): Column = new Column(
    ArraySampleIndex(a.expr, b.expr)
  )

  def array_sample_index(a: Column, b: Int): Column =
    array_sample_index(a, lit(b))

  def array_sort_by(a: Column, f: Column => Column) = new Column(
    ArraySortBy(a.expr, createLambda(f))
  )

  def array_split(a: Column, f: (Column, Column) => Column): Column = new Column(
    ArraySplit(a.expr, createLambda(f))
  )

  def continuous_dynamic_time_warping(a: Column, b: Column): Column = new Column(
    ContinuousDynamicTimeWarping(a.expr, b.expr)
  )

  def continuous_dynamic_time_warping_match(a: Column, b: Column): Column = new Column(
    ContinuousDynamicTimeWarpingMatch(a.expr, b.expr)
  )

  def continuous_frechet_distance(a: Column, b: Column): Column = new Column(
    ContinuousFrechetDistance(a.expr, b.expr)
  )

  def continuous_frechet_distance_match(a: Column, b: Column): Column = new Column(
    ContinuousFrechetDistanceMatch(a.expr, b.expr)
  )

  def continuous_hausdorff_distance(a: Column, b: Column): Column = new Column(
    ContinuousHausdorffDistance(a.expr, b.expr)
  )

  def continuous_hausdorff_distance_match(a: Column, b: Column): Column = new Column(
    ContinuousHausdorffDistanceMatch(a.expr, b.expr)
  )

  def discrete_dynamic_time_warping(a: Column, b: Column): Column = new Column(
    DiscreteDynamicTimeWarping(a.expr, b.expr)
  )

  def discrete_frechet_distance(a: Column, b: Column): Column = new Column(
    DiscreteFrechetDistance(a.expr, b.expr)
  )

  def discrete_hausdorff_distance(a: Column, b: Column): Column = new Column(
    DiscreteHausdorffDistance(a.expr, b.expr)
  )

  def euclidean_distance(a: Column, b: Column): Column = new Column(
    EuclideanDistance(a.expr, b.expr)
  )

  def levenshtein_distance(a: Column, b: Column): Column = new Column(
    LevenshteinDistance(a.expr, b.expr)
  )

  def mean_center(a: Column) = new Column(
    MeanCenter(a.expr)
  )

  def polyline_length(a: Column): Column = new Column(
    PolylineLength(a.expr)
  )

  def polyline_sample_equidistant(a: Column, b: Column): Column = new Column(
    PolylineSampleEquidistant(a.expr, b.expr)
  )

  def polyline_sample_equidistant(a: Column, b: Int): Column =
    polyline_sample_equidistant(a, lit(b))

  def smallest_circle_center(a: Column) = new Column(
    SmallestCircleCenter(a.expr)
  )

  def smallest_rectangle_center(a: Column) = new Column(
    SmallestRectangleCenter(a.expr)
  )

  case class FunctionInfo[T <: Expression]()(implicit classTag: ClassTag[T], companion: ExpressionCompanion[T]) {
    val runtimeClass: Class[?] = classTag.runtimeClass
    def prettyName: String = companion.prettyName
    def builder: Seq[Expression] => Expression = companion.apply
  }

  val functionInfoSeq: Seq[FunctionInfo[?]] = Seq(
    FunctionInfo[ArrayConsecutiveDistinct](),
    FunctionInfo[ArrayFilterSplit](),
    FunctionInfo[ArrayFilterSplitWithoutEnds](),
    FunctionInfo[ArraySampleIndex](),
    FunctionInfo[ArraySortBy](),
    FunctionInfo[ArraySplit](),
    FunctionInfo[ContinuousDynamicTimeWarping](),
    FunctionInfo[ContinuousDynamicTimeWarpingMatch](),
    FunctionInfo[ContinuousFrechetDistance](),
    FunctionInfo[ContinuousFrechetDistanceMatch](),
    FunctionInfo[ContinuousHausdorffDistance](),
    FunctionInfo[ContinuousHausdorffDistanceMatch](),
    FunctionInfo[DiscreteDynamicTimeWarping](),
    FunctionInfo[DiscreteFrechetDistance](),
    FunctionInfo[DiscreteHausdorffDistance](),
    FunctionInfo[EuclideanDistance](),
    FunctionInfo[LevenshteinDistance](),
    FunctionInfo[MeanCenter](),
    FunctionInfo[PolylineLength](),
    FunctionInfo[PolylineSampleEquidistant](),
    FunctionInfo[SmallestCircleCenter](),
    FunctionInfo[SmallestRectangleCenter]()
  )

  def registerTrajectoryFunctions(registry: FunctionRegistry): Unit = {
    for (info <- functionInfoSeq) {
      registry.registerFunction(
        FunctionIdentifier(info.prettyName),
        new ExpressionInfo(info.runtimeClass.getCanonicalName, info.prettyName),
        info.builder
      )
    }
  }

  private def createLambda(f: Column => Column): LambdaFunction = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    LambdaFunction(f(new Column(x)).expr, Seq(x))
  }

  private def createLambda(f: (Column, Column) => Column): LambdaFunction = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("y")))
    LambdaFunction(f(new Column(x), new Column(y)).expr, Seq(x, y))
  }
}
