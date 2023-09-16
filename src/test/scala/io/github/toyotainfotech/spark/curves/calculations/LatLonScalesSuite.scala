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

class LatLonScalesSuite extends AnyFunSuite with Matchers {
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.001)
  implicit val tuple2DoubleEquality: Equality[(Double, Double)] = (a: (Double, Double), b: Any) =>
    b match {
      case bt @ (_: Double, _: Double) =>
        doubleEquality.areEqual(a._1, bt._1) && doubleEquality.areEqual(a._2, bt._2)
      case _ =>
        false
    }

  test("LonLatScales") {
    // https://vldb.gsi.go.jp/sokuchi/surveycalc/surveycalc/bl2stf.html
    // TOKYO SKYTREE -> TOKYO TOWER
    LonLatScales.hubenyDistanceMeter(139.8107, 35.7101, 139.7454, 35.6586) shouldEqual 8221.402

    val scales = LonLatScales.hubenyScales(135, 35)
    scales.degreeToMeter(139.8, 35.7) shouldEqual (438183.214, 77658.402)
    scales.meterToDegree(438183.214, 77658.402) shouldEqual (139.8, 35.7)
  }
}
