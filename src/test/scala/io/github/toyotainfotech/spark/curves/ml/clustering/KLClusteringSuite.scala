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
package io.github.toyotainfotech.spark.curves.ml.clustering

import io.github.toyotainfotech.spark.curves.SparkSuites
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

object KLClusteringSuite {
  case class XY(
      x: Double,
      y: Double
  )
  case class Trajectory(
      id: String,
      curve: Seq[XY]
  )
  case class ClusteredTrajectory(
      id: String,
      curve: Seq[XY],
      clusterIndex: Int,
      similarity: Double
  )
}

@DoNotDiscover
class KLClusteringSuite extends AnyFunSuite with Matchers {
  import KLClusteringSuite.*
  import SparkSuites.spark.implicits.*

  test("KLCluster") {
    val trajectories = {
      Random
        .shuffle(
          Seq(
            Trajectory("a", (0 to 100).map(a => XY(-0.5 + a * 0.01, -1.0))),
            Trajectory("b", (0 to 100).map(a => XY(-0.5 + a * 0.01, 1.0))),
            Trajectory("c", (0 to 100).map(a => XY(-0.5 + a * 0.01, 2.0)))
          )
        )
        .toDS()
    }

    val klClustering = new KLClustering()
      .setK(2)
      .setL(16)
      .setIter(2)
      .setCurveSimilarityFunc("continuous_frechet_distance")
      .setCurveSimplifyFunc("polyline_sample_equidistant")
      .setPointsCenterFunc("smallest_circle_center")
      .setInputCurveCol("curve")
      .setInputCurveAxisFields(Array("x", "y"))
      .setOutputClusterIndexCol("clusterIndex")
      .setOutputSimilarityCol("similarity")
      .setOutputMatchingCurveCol("matching_curve")

    val klClusterModel = klClustering.fit(trajectories)
    val result = klClusterModel.transform(trajectories)

    klClusterModel.transformSchema(trajectories.schema) shouldEqual result.schema

    val clusterIndexes = result.as[ClusteredTrajectory].collect().map(a => a.id -> a.clusterIndex).toMap
    clusterIndexes("a") shouldNot equal(clusterIndexes("b"))
    clusterIndexes("b") shouldEqual clusterIndexes("c")
  }
}
