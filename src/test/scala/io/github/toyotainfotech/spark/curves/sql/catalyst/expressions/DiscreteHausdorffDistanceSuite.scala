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
class DiscreteHausdorffDistanceSuite extends SparkSqlFuncSuite {
  override val funcName: String = DiscreteHausdorffDistance.prettyName
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test(funcName) {
    sqlOneRowTest(
      4.123, /* (3, 0) - (4, 4) */
      "SELECT _FUNC_(array(array(1d, 0d), array(2d, 0d), array(3d, 0d)), array(array(1d, 1d), array(2d, 2d), array(3d, 3d), array(4d, 4d)))"
    )
    sqlOneRowTest(
      0.361, /* (0.3, 0.4) - (0.1, 0.1)*/
      "SELECT _FUNC_(array(array(-0.1d, -0.1d), array(0.3d, -0.1d), array(0.3d, 0.4d), array(-0.1d, 0.4d)), array(array(0d, 0d), array(0.1d, 0d), array(0.1d, 0.1d), array(0d, 0.1d)))"
    )
  }
}
