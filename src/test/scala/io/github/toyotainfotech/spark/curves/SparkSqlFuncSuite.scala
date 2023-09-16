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
package io.github.toyotainfotech.spark.curves

import org.apache.spark.sql.SparkSession
import org.scalactic.Equality
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

trait SparkSqlFuncSuite extends AnyFunSuite with Matchers {
  val spark: SparkSession = SparkSuites.spark
  val funcName: String

  def sqlOneRowTest[T: Equality](answer: T, sqlText: String): Unit =
    answer.shouldEqual(spark.sql(sqlText.replace("_FUNC_", funcName)).collect().head.get(0))

  implicit def seqEquality[T](implicit equality: Equality[T]): Equality[Seq[T]] =
    (a: Seq[T], b: Any) =>
      b match {
        // Spark array type:
        // - Scala 2.12: mutable.WrappedArray
        // - Scala 2.13: mutable.ArraySeq
        case bSeq: mutable.Seq[?] =>
          (a.length == bSeq.length) && a.zip(bSeq).forall { case (ae, be) => equality.areEqual(ae, be) }

        case _ =>
          false
      }
}
