package libs.etl.silver.airbnb.dimTable


import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{explode, from_json, col, lower}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, ArrayType}

object TbImages {

    def get(airbnb_dataframe: DataFrame): DataFrame = {
        airbnb_dataframe.select("_id","images.thumbnail_url","images.medium_url","images.picture_url","images.xl_picture_url")

    }
  
}
