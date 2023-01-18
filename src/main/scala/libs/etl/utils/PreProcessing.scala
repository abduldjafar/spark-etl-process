package libs.etl.utils

import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{Column}
import org.apache.spark.sql.types.{StringType, StructType, ArrayType}

object PreProcessing {
  def flattenStructSchema(
      schema: StructType,
      prefix: String = null
  ): Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ =>
          Array(
            col(columnName).as(columnName.replace(".", "_").replace("$", ""))
          )
      }

    })
  }
}
