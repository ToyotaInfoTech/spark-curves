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

object EuclideanSpace {
  def sqdist(a: Array[Double], b: Array[Double]): Double =
    if (a.length != b.length) {
      Double.NaN
    } else {
      var result = 0.0
      var i = 0
      while (i < a.length) {
        val d = a(i) - b(i)
        result += d * d
        i += 1
      }
      result
    }

  def dist(a: Array[Double], b: Array[Double]): Double =
    math.sqrt(sqdist(a, b))

  def interiorPoint(a: Array[Double], b: Array[Double], rate: Double): Array[Double] =
    if (a.length != b.length) {
      null
    } else if (rate <= 0) {
      a
    } else if (rate >= 1) {
      b
    } else {
      val result = new Array[Double](a.length)
      var i = 0
      while (i < a.length) {
        result(i) = a(i) * (1 - rate) + b(i) * rate
        i += 1
      }
      result
    }

  def nearestInteriorPoint(a: Array[Double])(b: Array[Double], c: Array[Double]): Array[Double] =
    if (a.length != b.length || a.length != c.length) {
      null
    } else {
      val ba = new Array[Double](a.length)
      val bc = new Array[Double](a.length)
      var bcbc = 0.0
      var rate = 0.0
      var i = 0
      while (i < a.length) {
        ba(i) = a(i) - b(i)
        bc(i) = c(i) - b(i)
        rate += ba(i) * bc(i)
        bcbc += bc(i) * bc(i)
        i += 1
      }
      if (bcbc > 0) {
        rate /= bcbc
        interiorPoint(b, c, rate)
      } else {
        b
      }
    }
}
