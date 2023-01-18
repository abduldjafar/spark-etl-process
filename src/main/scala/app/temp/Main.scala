import org.apache.spark.sql.SparkSession
import libs.etl.silver.mflix.dimTable
import libs.etl.utils.AfterProcessing
import libs.dqc.silver.SilverDqc
import libs.etl.silver.mflix.dimTable.{
  TbCast,
  TbCountry,
  TbDirectors,
  TbGenre,
  TbLanguage,
  TbWriters
}
import libs.etl.silver.mflix.factTable.MoviesFact
import app.Init

object Main {
  

  def main(args: Array[String]): Unit = {
    val slack_url = args(0)
    
    val use_aws_in_local =  if ( args(1) == "true" ) true else false

    val init  = new Init("spark Etl")

    val spark = init.sessionSPark(hiveSupport = true,useAwsEnv=use_aws_in_local)
    
    
    var mflix_datas = spark.read.parquet(
      init.mflix_movies_s3_path
    )


    

    /**
    // generate movies genres table
    var mflix_genres_tables = TbGenre.get(mflix_datas)

    AfterProcessing.writeToS3asParquet(
      mflix_genres_tables,
      mflix_movies_genres_s3_path,
      "overwrite",
      mflix_movie_genres_tb_location,
      "genre",
      true
    )

    SilverDqc.dqcTbGenre(
      spark,
      mflix_movies_genres_s3_path,
      slack_url,
      mflix_movie_genres_tb_location
    )

    // generate movies genres cast
    var mflix_cast_tables = TbCast.get(mflix_datas)
    AfterProcessing.writeToS3asParquet(
      mflix_cast_tables,
      mflix_movies_cast_s3_path,
      "overwrite",
      mflix_movie_cast_tb_location,
      "cast",
      false
    )

    SilverDqc.dqcTbCast(
      spark,
      mflix_movies_cast_s3_path,
      slack_url,
      mflix_movie_cast_tb_location
    )

    // generate movies country
    var mflix_countries_tables = TbCountry.get(mflix_datas)
    AfterProcessing.writeToS3asParquet(
      mflix_countries_tables,
      mflix_movies_countries_s3_path,
      "overwrite",
      mflix_movie_countries_tb_location,
      "",
      false
    )

    SilverDqc.dqcTbCountries(
      spark,
      mflix_movies_countries_s3_path,
      slack_url,
      mflix_movie_countries_tb_location
    )

    // generate movies directors
    var mflix_directors_tables = TbDirectors.get(mflix_datas)
    AfterProcessing.writeToS3asParquet(
      mflix_directors_tables,
      mflix_movies_directors_s3_path,
      "overwrite",
      mflix_movie_directors_tb_location,
      "",
      false
    )

    SilverDqc.dqcTbDirectors(
      spark,
      mflix_movies_directors_s3_path,
      slack_url,
      mflix_movie_directors_tb_location
    )

    // generate movies languages
    var mflix_languages_tables = TbLanguage.get(mflix_datas)
    AfterProcessing.writeToS3asParquet(
      mflix_languages_tables,
      mflix_movies_languages_s3_path,
      "overwrite",
      mflix_movie_languages_tb_location,
      "",
      false
    )

    SilverDqc.dqcTbLanguages(
      spark,
      mflix_movies_languages_s3_path,
      slack_url,
      mflix_movie_languages_tb_location
    )

    // generate movies writers
    var mflix_writers_tables = TbWriters.get(mflix_datas)
    AfterProcessing.writeToS3asParquet(
      mflix_writers_tables,
      mflix_movies_writers_s3_path,
      "overwrite",
      mflix_movie_writers_tb_location,
      "",
      false
    )

    SilverDqc.dqcTbWriters(
      spark,
      mflix_movies_writers_s3_path,
      slack_url,
      mflix_movie_writers_tb_location
    )

    // generate movies facts
    var mflix_movies_fact_tables = MoviesFact.get(mflix_datas)
    AfterProcessing.writeToS3asParquet(
      mflix_movies_fact_tables,
      mflix_movies_movies_fact_s3_path,
      "overwrite",
      mflix_movie_movies_fact_tb_location,
      "",
      false
    )

    SilverDqc.dqcTbMovieFacts(
      spark,
      mflix_movies_movies_fact_s3_path,
      slack_url,
      mflix_movie_movies_fact_tb_location
    )
      **/
  }

}
