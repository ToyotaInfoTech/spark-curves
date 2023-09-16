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
class ArraySplitSuite extends SparkSqlFuncSuite {
  override val funcName: String = ArraySplit.prettyName

  test(funcName) {
    sqlOneRowTest(
      Seq(Seq(Seq(0, 1), Seq(1, 2), Seq(2, 4)), Seq(Seq(3, 3), Seq(4, 5)), Seq(Seq(5, 0))),
      "SELECT _FUNC_(array(array(0, 1), array(1, 2), array(2, 4), array(3, 3), array(4, 5), array(5, 0)), (a, b) -> a[1] > b[1])"
    )
    sqlOneRowTest(
      Seq(),
      "SELECT _FUNC_(array(), (a, b) -> a > b)"
    )
  }
}
