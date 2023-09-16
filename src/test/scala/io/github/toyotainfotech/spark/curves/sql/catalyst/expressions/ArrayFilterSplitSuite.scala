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
import org.scalatest.DoNotDiscover

@DoNotDiscover
class ArrayFilterSplitSuite extends SparkSqlFuncSuite {
  override val funcName: String = ArrayFilterSplit.prettyName

  test(funcName) {
    sqlOneRowTest(
      Seq(Seq(1, 2), Seq(5, 6, 7), Seq(9)),
      "SELECT _FUNC_(array(1, 2, -3, -4, 5, 6, 7, -8, 9), a -> a > 0)"
    )
    sqlOneRowTest(
      Seq(Seq(4, 5), Seq(7, 8)),
      "SELECT _FUNC_(array(-1, -2, -3, 4, 5, -6, 7, 8, -9), a -> a > 0)"
    )
    sqlOneRowTest(
      Seq(),
      "SELECT _FUNC_(array(), a -> a > 0)"
    )
  }
}
