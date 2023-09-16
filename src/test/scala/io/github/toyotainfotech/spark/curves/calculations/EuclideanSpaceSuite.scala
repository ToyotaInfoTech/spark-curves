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
package io.github.toyotainfotech.spark.curves.calculations

import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EuclideanSpaceSuite extends AnyFunSuite with Matchers {
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test("EuclideanSpace") {
    EuclideanSpace.dist(Array(1, 2, 3), Array(1, -1, 7)) shouldEqual 5.0
  }
}
