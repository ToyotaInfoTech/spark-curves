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
class PolylineSampleEquidistantSuite extends SparkSqlFuncSuite {
  override val funcName: String = PolylineSampleEquidistant.prettyName
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test(funcName) {
    sqlOneRowTest(
      Seq(Seq(0.0, 0.0), Seq(2.5, 1.0), Seq(3.0, -2.0), Seq(3.0, -5.5), Seq(3.0, -9.0)),
      "SELECT _FUNC_(array(array(0d, 0d), array(0d, 1d), array(3d, 1d), array(3d, -9d)), 5)"
    )
    sqlOneRowTest(
      Seq(Seq(1.0, 2.0), Seq(1.0, 2.0), Seq(1.0, 2.0), Seq(1.0, 2.0)),
      "SELECT _FUNC_(array(array(1d, 2d), array(1d, 2d)), 4)"
    )
    sqlOneRowTest(
      Seq(Seq(1.0, 2.0)),
      "SELECT _FUNC_(array(array(1d, 2d), array(3d, 4d)), 1)"
    )
    sqlOneRowTest(
      Seq(),
      "SELECT _FUNC_(array(array(1d, 2d), array(3d, 4d)), 0)"
    )
    sqlOneRowTest(
      Seq(Seq(1.0, 2.0), Seq(1.0, 2.0), Seq(1.0, 2.0)),
      "SELECT _FUNC_(array(array(1d, 2d)), 3)"
    )
    sqlOneRowTest(
      Seq(),
      "SELECT _FUNC_(cast(array() as array<array<double>>), 3)"
    )
  }
}
