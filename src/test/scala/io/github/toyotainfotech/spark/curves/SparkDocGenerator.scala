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

import io.github.toyotainfotech.spark.curves.ml.clustering.KLClustering
import io.github.toyotainfotech.spark.curves.sql.functions.functionInfoSeq
import org.apache.spark.ml.param.*
import org.apache.spark.sql.catalyst.expressions.ExpressionDescription
import org.scalatest.funsuite.AnyFunSuite

import java.io.PrintWriter

class SparkDocGenerator extends AnyFunSuite {
  test(getClass.getSimpleName) {
    val mlDoc = s"# API\n\nThis file was generated by ${getClass.getSimpleName}." +
      Seq(new KLClustering())
        .groupBy(_.getClass.getName.split('.').dropRight(1).mkString("."))
        .toSeq
        .map { case (k, vs) => s"## $k\n\n" + vs.sortBy(_.getClass.getSimpleName).map(mlParams).mkString("\n\n") }
        .mkString("\n\n", "\n\n", "\n\n")

    val sqlDoc = "## io.github.toyotainfotech.spark.curves.sql.functions" +
      functionInfoSeq
        .sortBy(_.prettyName)
        .flatMap(info =>
          Option(info.runtimeClass.getAnnotation(classOf[ExpressionDescription])).map((info.prettyName, _))
        )
        .map { case (name, annotation) =>
          s"### $name" +
            sqlDescription(annotation.usage().replace("_FUNC_", name))
              .map("\n\n" + _.mkString("\n"))
              .getOrElse("") +
            sqlDescription(annotation.arguments())
              .map("\n\n#### Arguments:\n\n" + _.tail.mkString("\n").replace("<", "\\<").replace(">", "\\>"))
              .getOrElse("") +
            sqlDescription(annotation.examples().replace("_FUNC_", name))
              .map("\n\n#### Examples:\n\n```\n" + _.tail.mkString("\n") + "\n```")
              .getOrElse("")
        }
        .mkString("\n\n", "\n\n", "\n\n")

    val writer = new PrintWriter("docs/generated/api.md")
    try {
      writer.write(mlDoc + sqlDoc)
    } finally {
      writer.close()
    }
  }

  private def mlParams(params: Params): String =
    s"### ${params.getClass.getSimpleName}\n\n" +
      mlTable(
        Seq("parameter", "type", "default", "description"),
        params.params.toSeq.sortBy(_.name).map(mlParam(params, _))
      )

  private def mlParam[T](params: Params, param: Param[T]): Seq[String] = {
    val typeName: String = param match {
      case _: BooleanParam          => "Boolean"
      case _: DoubleArrayArrayParam => "Array[Array[Double]]"
      case _: DoubleArrayParam      => "Array[Double]"
      case _: DoubleParam           => "Double"
      case _: FloatParam            => "Float"
      case _: IntArrayParam         => "Array[Int]"
      case _: IntParam              => "Int"
      case _: LongParam             => "Long"
      case _: StringArrayParam      => "Array[String]"
      case _                        => "String"
    }
    val defaultName = params.getDefault(param).map(_.toString).getOrElse("")
    Seq(param.name, typeName, defaultName, param.doc)
  }

  private def mlTable(header: Seq[String], rows: Seq[Seq[String]]): String = {
    assert(rows.forall(_.size == header.size))
    header
      .zip(rows.transpose)
      .map { case (name, col) => Seq(" " + name + " ", " --- ") ++ col.map(" " + _ + " ") }
      .transpose
      .map(_.mkString("|", "|", "|"))
      .mkString("\n")
  }

  private def sqlDescription(description: String): Option[Seq[String]] = {
    if (description == "") None
    else Some(description.linesIterator.map(_.dropWhile(_ == ' ')).filter(_ != "").toSeq)
  }
}
