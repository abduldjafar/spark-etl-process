package app.etl.mflix

import libs.etl.utils.AfterProcessing
import libs.etl.silver.mflix.dimTable.TbGenre
import libs.dqc.silver.SilverDqc
import app.Init


object ProcessMflixTbGenres {
  def main(args: Array[String]): Unit = {
    val slack_url = args(0)
    val hiveSupport = if (args(1) == "true") true else false
    val useAwsEnv = if (args(2) == "true") true else false

    val init  = new Init("spark Etl")
    val spark = init.sessionSPark(hiveSupport = hiveSupport,useAwsEnv=useAwsEnv)
    
    
    var mflix_datas = spark.read.parquet(
      init.mflix_movies_s3_path
    )


    var mflix_genres_tables = TbGenre.get(mflix_datas)

    mflix_genres_tables.show()
    
    AfterProcessing.writeToS3asParquet(
      mflix_genres_tables,
      init.mflix_movies_genres_s3_path,
      "overwrite",
      init.mflix_movie_genres_tb_location,
      "genre",
      true,
      hiveSupport
    )
   
  }
}
