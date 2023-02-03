package app

import org.apache.spark.sql.SparkSession
import com.amazonaws.auth.{
  AWSCredentialsProviderChain,
  EnvironmentVariableCredentialsProvider,
  profile,
  AWSCredentialsProvider
}

class Init (val name: String) {

  val mflix_movies_s3_path =
    "s3a://quipper-datalake-dev/bronze/sample_mflix/movies/*"
  val mflix_movies_genres_s3_path =
    "s3a://quipper-datalake-dev/silver/mflix/genres/"
  val mflix_movies_cast_s3_path =
    "s3a://quipper-datalake-dev/silver/mflix/cast/"
  val mflix_movies_countries_s3_path =
    "s3a://quipper-datalake-dev/silver/mflix/countries/"
  val mflix_movies_directors_s3_path =
    "s3a://quipper-datalake-dev/silver/mflix/directors/"
  val mflix_movies_languages_s3_path =
    "s3a://quipper-datalake-dev/silver/mflix/languages/"
  val mflix_movies_writers_s3_path =
    "s3a://quipper-datalake-dev/silver/mflix/writers/"
  val mflix_movies_movies_fact_s3_path =
    "s3a://quipper-datalake-dev/silver/mflix/movies_fact/"
  
  val airbnb_listing_reviews_datas_s3_path = "s3a://quipper-datalake-dev/bronze/sample_airbnb/listingsAndReviews/LOAD00000001.parquet"

  val mflix_movie_genres_tb_location = "mflix.tb_genres"
  val mflix_movie_cast_tb_location = "mflix.tb_cast"
  val mflix_movie_countries_tb_location = "mflix.tb_countries"
  val mflix_movie_directors_tb_location = "mflix.tb_directors"
  val mflix_movie_languages_tb_location = "mflix.tb_languages"
  val mflix_movie_writers_tb_location = "mflix.tb_writers"
  val mflix_movie_movies_fact_tb_location = "mflix.tb_movies_fact"

  def sparkBuilder (): SparkSession.Builder = {
    SparkSession
      .builder ()
      .appName (this.name)
  }

  def sessionSPark (hiveSupport: Boolean, useAwsEnv: Boolean): SparkSession = {
    var sparkBuilder = this.sparkBuilder ()

    if (hiveSupport) {
      sparkBuilder = this
        .sparkBuilder ()
        .enableHiveSupport ()

    }

    if (useAwsEnv) {
      val awsAccessKeyId = System.getenv ("AWS_ACCESS_KEY_ID")
      val awsSecretKey = System.getenv ("AWS_SECRET_ACCESS_KEY")

      sparkBuilder
        .config ("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config ("spark.hadoop.fs.s3a.access.key", awsAccessKeyId)
        .config ("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config (
          "spark.hadoop.fs.s3a.secret.key", awsSecretKey
        )
    }

    sparkBuilder.getOrCreate ()
  }

}
