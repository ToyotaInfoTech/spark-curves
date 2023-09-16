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

import io.github.toyotainfotech.spark.curves.SparkSqlFuncSuite
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.DoNotDiscover

@DoNotDiscover
class ContinuousHausdorffDistanceSuite extends SparkSqlFuncSuite {
  override val funcName: String = ContinuousHausdorffDistance.prettyName
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test(funcName) {
    sqlOneRowTest(
      2.0,
      "SELECT _FUNC_(array(array(1d, 0d), array(2d, 0d), array(3d, 0d), array(4d, 0d), array(5d, 0d), array(3d, 0d)), array(array(4d, 0d), array(3d, 2d), array(1d, 1d)))"
    )
  }
}

@DoNotDiscover
class ContinuousHausdorffDistanceMatchSuite extends SparkSqlFuncSuite {
  override val funcName: String = ContinuousHausdorffDistanceMatch.prettyName
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test(funcName) {
    sqlOneRowTest(
      Seq(Seq(1.0, 1.0), Seq(1.4, 1.2), Seq(3.8, 0.4), Seq(4.0, 0.0), Seq(4.0, 0.0), Seq(3.8, 0.4)),
      "SELECT _FUNC_(array(array(1d, 0d), array(2d, 0d), array(3d, 0d), array(4d, 0d), array(5d, 0d), array(3d, 0d)), array(array(4d, 0d), array(3d, 2d), array(1d, 1d)))"
    )
  }
}
