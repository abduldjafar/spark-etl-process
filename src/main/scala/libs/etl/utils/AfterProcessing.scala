package libs.etl.utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{Column, DataFrame,DataFrameWriter,Row}
import org.apache.spark.sql.types.{StringType, StructType, ArrayType}

object AfterProcessing {

  def writeToS3asParquet(
      datas: DataFrame,
      s3Path: String,
      mode: String = "overwrite",
      tb_in_catalog: String,
      partition_col: String = "",
      is_partitioned: Boolean,
      save_in_catalog: Boolean
  ): Unit = {
    var dataFramewriter = datas.write
    if(is_partitioned){
      dataFramewriter = dataFramewriter
      .format("parquet")
      .partitionBy(partition_col)
      .option("path", s3Path)
      .mode(mode)
      
    }else{
      dataFramewriter = dataFramewriter
      .format("parquet")
      .option("path", s3Path)
      .mode(mode)
      
    }

    if (save_in_catalog){
      dataFramewriter.saveAsTable(tb_in_catalog)
    }else{
      dataFramewriter.save()
    }
    
    
    
  }

  def writeToS3(
      datas: DataFrame,
      s3Path: String,
      mode: String = "overwrite",
      tb_in_catalog: String,
      partition_col: String = ""
  ): Unit = {
    datas.write
      .format("parquet")
      .partitionBy(partition_col)
      .option("path", s3Path)
      .mode(mode)
      .saveAsTable(tb_in_catalog)
  }
}
