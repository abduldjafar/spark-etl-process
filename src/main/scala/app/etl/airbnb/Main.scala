package app.etl.airbnb
import app.Init

object Main {
  def main(args: Array[String]): Unit = {
    val slack_url = args(0)
    val hiveSupport = if (args(1) == "true") true else false
    val useAwsEnv = if (args(2) == "true") true else false

    val init  = new Init("spark Etl")
    val spark = init.sessionSPark(hiveSupport = hiveSupport,useAwsEnv=useAwsEnv)
    
    
    var airbnb_datas = spark.read.parquet(
      init.airbnb_listing_reviews_datas_s3_path
    )

    airbnb_datas.printSchema()
  }
}
