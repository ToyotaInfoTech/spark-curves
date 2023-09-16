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

case class LonLatScales(baseLongitude: Double, baseLatitude: Double, xMeterPerDegree: Double, yMeterPerDegree: Double) {
  def degreeToMeter(longitude: Double, latitude: Double): (Double, Double) =
    ((longitude - baseLongitude) * xMeterPerDegree, (latitude - baseLatitude) * yMeterPerDegree)

  def meterToDegree(x: Double, y: Double): (Double, Double) =
    ((x / xMeterPerDegree) + baseLongitude, (y / yMeterPerDegree) + baseLatitude)
}

object LonLatScales {
  // cf. https://www.trail-note.net/tech/calc_distance/
  def hubenyFormula(baseLatitude: Double): (Double, Double) = {
    // WGS84
    val rx = 6378137.0
    val ry = 6356752.314245
    val e = math.sqrt(1 - (ry * ry / (rx * rx)))
    val esin = e * math.sin(math.toRadians(baseLatitude))
    val w = math.sqrt(1 - esin * esin)
    val n = rx / w
    val m = rx * (1 - e * e) / (w * w * w)
    (math.toRadians(1) * n * math.cos(math.toRadians(baseLatitude)), math.toRadians(1) * m)
  }

  def hubenyScales(baseLongitude: Double, baseLatitude: Double): LonLatScales = {
    val (xMeterPerDegree, yMeterPerDegree) = hubenyFormula(baseLatitude)
    LonLatScales(baseLongitude, baseLatitude, xMeterPerDegree, yMeterPerDegree)
  }

  def hubenyDistanceMeter(longitude1: Double, latitude1: Double, longitude2: Double, latitude2: Double): Double = {
    val (xMeterPerDegree, yMeterPerDegree) = hubenyFormula((latitude1 + latitude2) / 2)
    val dx = (longitude1 - longitude2) * xMeterPerDegree
    val dy = (latitude1 - latitude2) * yMeterPerDegree
    math.sqrt(dx * dx + dy * dy)
  }
}
