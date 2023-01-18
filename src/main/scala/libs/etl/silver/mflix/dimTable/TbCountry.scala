package libs.etl.silver.mflix.dimTable
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{explode, from_json, col, lower}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, ArrayType}

object TbCountry {
  def get(mflix_dataframe: DataFrame): DataFrame = {
    var result = mflix_dataframe.select("oid__id", "array_countries")

    result
      .withColumn(
        "array_countries",
        explode(
          from_json(col("array_countries"), ArrayType(StringType))
        )
      )
      .withColumnRenamed("oid__id", "movie_id")
      .withColumnRenamed("array_countries", "countries")
      .withColumn("countries", lower(col("countries")))
  }
}
