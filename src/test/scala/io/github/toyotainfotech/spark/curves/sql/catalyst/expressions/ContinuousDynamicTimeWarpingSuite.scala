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
class ContinuousDynamicTimeWarpingSuite extends SparkSqlFuncSuite {
  override val funcName: String = ContinuousDynamicTimeWarping.prettyName
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test(funcName) {
    sqlOneRowTest(
      2.722,
      "SELECT _FUNC_(array(array(-1d, 1d), array(0d, 1d), array(0d, 2d), array(1d, 2d), array(2d, 2d), array(3d, 2d), array(3d, 1d), array(4d, 1d)), array(array(-1d, 0d), array(0d, 0d), array(3d, 0d), array(4d, 0d)))"
    )
    sqlOneRowTest(
      0.0,
      "SELECT _FUNC_(array(array(1d, 1d), array(2d, 1d), array(2d, 2d)), array(array(1d, 1d), array(2d, 1d), array(2d, 2d)))"
    )
    sqlOneRowTest(
      0.881,
      "SELECT _FUNC_(array(array(1d, 1d), array(3d, 1d), array(5d, 1d), array(7d, 1d)), array(array(0d, 1d), array(2d, 0d), array(4d, 0d), array(6d, 0d), array(7d, 0d)))"
    )
  }
}

@DoNotDiscover
class ContinuousDynamicTimeWarpingMatchSuite extends SparkSqlFuncSuite {
  override val funcName: String = ContinuousDynamicTimeWarpingMatch.prettyName
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test(funcName) {
    sqlOneRowTest(
      Seq(
        Seq(-1.0, 0.0),
        Seq(0.0, 0.0),
        Seq(0.0, 0.0),
        Seq(1.0, 0.0),
        Seq(2.0, 0.0),
        Seq(3.0, 0.0),
        Seq(3.0, 0.0),
        Seq(4.0, 0.0)
      ),
      "SELECT _FUNC_(array(array(-1d, 1d), array(0d, 1d), array(0d, 2d), array(1d, 2d), array(2d, 2d), array(3d, 2d), array(3d, 1d), array(4d, 1d)), array(array(-1d, 0d), array(0d, 0d), array(3d, 0d), array(4d, 0d)))"
    )
    sqlOneRowTest(
      Seq(
        Seq(-1.0, 1.0),
        Seq(0.0, 1.0),
        Seq(3.0, 1.0),
        Seq(4.0, 1.0)
      ),
      "SELECT _FUNC_(array(array(-1d, 0d), array(0d, 0d), array(3d, 0d), array(4d, 0d)), array(array(-1d, 1d), array(0d, 1d), array(0d, 2d), array(1d, 2d), array(2d, 2d), array(3d, 2d), array(3d, 1d), array(4d, 1d)))"
    )
    sqlOneRowTest(
      Seq(
        Seq(1.0, 0.0),
        Seq(2.0, 0.0),
        Seq(3.0, 0.0),
        Seq(2.0, 0.0)
      ),
      "SELECT _FUNC_(array(array(1d, 1d), array(2d, 1d), array(3d, 1d), array(4d, 1d)), array(array(1d, 0d), array(0d, 0d), array(0.5d, 0d), array(2d, 0d), array(2d, -1d), array(2d, 0d), array(3.5d, 0d), array(3.5d, -1d), array(3.5d, 0d), array(4.5d, 0d), array(5d, 0d), array(2d, 0d)))"
    )
    sqlOneRowTest(
      Seq(
        Seq(1.0, 1.0),
        Seq(1.0, 1.0),
        Seq(1.0, 1.0),
        Seq(2.0, 1.0),
        Seq(2.0, 1.0),
        Seq(2.0, 1.0),
        Seq(3.5, 1.0),
        Seq(3.5, 1.0),
        Seq(3.5, 1.0),
        Seq(4.0, 1.0),
        Seq(4.0, 1.0),
        Seq(4.0, 1.0)
      ),
      "SELECT _FUNC_(array(array(1d, 0d), array(0d, 0d), array(0.5d, 0d), array(2d, 0d), array(2d, -1d), array(2d, 0d), array(3.5d, 0d), array(3.5d, -1d), array(3.5d, 0d), array(4.5d, 0d), array(5d, 0d), array(2d, 0d)), array(array(1d, 1d), array(2d, 1d), array(3d, 1d), array(4d, 1d)))"
    )
  }
}
