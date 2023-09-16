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
class PolylineLengthSuite extends SparkSqlFuncSuite {
  override val funcName: String = PolylineLength.prettyName
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test(funcName) {
    sqlOneRowTest(
      0.724,
      "SELECT _FUNC_(array(array(0.0d, 0.0d), array(0.1d, 0.2d), array(0.4d, -0.2d)))"
    )
    sqlOneRowTest(
      3.742,
      "SELECT _FUNC_(array(array(1d, 2d, 3d), array(2d, 4d, 6d)))"
    )
    sqlOneRowTest(
      0.0,
      "SELECT _FUNC_(cast(array() as array<array<double>>))"
    )
  }
}
