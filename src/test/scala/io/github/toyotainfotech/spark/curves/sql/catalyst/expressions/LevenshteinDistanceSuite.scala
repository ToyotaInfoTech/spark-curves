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
class LevenshteinDistanceSuite extends SparkSqlFuncSuite {
  override val funcName: String = LevenshteinDistance.prettyName

  test(funcName) {
    sqlOneRowTest(
      0,
      "SELECT _FUNC_(array('k', 'i', 't', 't', 'e', 'n'), array('k', 'i', 't', 't', 'e', 'n'))"
    )
    sqlOneRowTest(
      3,
      "SELECT _FUNC_(array('k', 'i', 't', 't', 'e', 'n'), array('s', 'i', 't', 't', 'i', 'n', 'g'))"
    )
  }
}
