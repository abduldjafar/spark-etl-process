package app.dqc.mflix

import app.Init
import libs.dqc.Dqc
import com.amazon.deequ.checks.{Check, CheckLevel}

object ProcessMflixTbGenres {
  def main (args: Array [String]): Unit = {
    val slack_url = args (0)
    val useAwsEnv = if (args (1) == "true") true else false

    val init = new Init ("spark Etl")
    val spark = init.sessionSPark (hiveSupport = false, useAwsEnv = useAwsEnv)

    val dqcForMflixTbGenres = new Dqc ()

    val result = dqcForMflixTbGenres.doTesting (spark, init.mflix_movies_genres_s3_path,
    Check (CheckLevel.Error, "mflix tb_genre")
      .isComplete ("movie_id")
      .hasMaxLength ("movie_id", _ == 24.0))

    dqcForMflixTbGenres.sendResultToSlack (slack_url, init.mflix_movie_genres_tb_location,
    result)
  }
}
