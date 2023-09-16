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
class ArraySortBySuite extends SparkSqlFuncSuite {
  override val funcName: String = ArraySortBy.prettyName

  test(funcName) {
    sqlOneRowTest(
      Seq(-1, 2, -3, -4, 5),
      "SELECT _FUNC_(array(5, -3, -1, 2 ,-4), a -> abs(a))"
    )
    sqlOneRowTest(
      Seq(Seq("C", "a"), Seq("A", "b"), Seq("B", "c")),
      "SELECT _FUNC_(array(array('C', 'a'), array('B', 'c'), array('A', 'b')), a -> a[1])"
    )
    sqlOneRowTest(
      Seq(),
      "SELECT _FUNC_(array(), a -> a > 0)"
    )
  }
}
