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

import org.apache.spark.sql.types.{StructField, StructType}

package object sql {
  implicit class StructTypeExtension(val schema: StructType) extends AnyVal {
    def withField(fields: StructField*): StructType = {
      val newFields = fields.toBuffer
      val updatedFields = for (sourceField <- schema.fields) yield {
        val i = newFields.indexWhere(_.name == sourceField.name)
        if (i < 0) {
          sourceField
        } else {
          newFields.remove(i)
        }
      }
      StructType(updatedFields ++ newFields)
    }
  }
}
