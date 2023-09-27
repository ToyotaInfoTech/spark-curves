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
package io.github.toyotainfotech.spark.curves

import io.github.toyotainfotech.spark.curves.ml.clustering.KLClusteringSuite
import io.github.toyotainfotech.spark.curves.sql.catalyst.expressions.*
import io.github.toyotainfotech.spark.curves.sql.functions.registerCurveFunctions
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suites}

class SparkSuites
    extends Suites(
      // ml.clustering.*
      new KLClusteringSuite,
      // sql.catalyst.*
      new ArrayConsecutiveDistinctSuite,
      new ArrayFilterSplitSuite,
      new ArrayFilterSplitWithoutEndsSuite,
      new ArraySampleIndexSuite,
      new ArraySortBySuite,
      new ArraySplitSuite,
      new ContinuousDynamicTimeWarpingSuite,
      new ContinuousDynamicTimeWarpingMatchSuite,
      new ContinuousFrechetDistanceSuite,
      new ContinuousFrechetDistanceMatchSuite,
      new ContinuousHausdorffDistanceSuite,
      new ContinuousHausdorffDistanceMatchSuite,
      new DiscreteDynamicTimeWarpingSuite,
      new DiscreteFrechetDistanceSuite,
      new DiscreteHausdorffDistanceSuite,
      new EuclideanDistanceSuite,
      new LevenshteinDistanceSuite,
      new MeanCenterSuite,
      new PolylineLengthSuite,
      new PolylineSampleEquidistantSuite,
      new SmallestCircleCenterSuite,
      new SmallestRectangleCenterSuite
    )
    with BeforeAndAfterAll {

  override def beforeAll(): Unit =
    registerCurveFunctions(SparkSuites.spark.sessionState.functionRegistry)

  override def afterAll(): Unit =
    SparkSuites.spark.stop()
}

object SparkSuites {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.log.level", "WARN")
    .getOrCreate()
}
