package libs.etl.silver.mflix.factTable

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{explode, from_json, col, lower}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, ArrayType}

object MoviesFact {
  def get(mflix_dataframe: DataFrame): DataFrame = {
    var result = mflix_dataframe.select(
      "oid__id",
      "num_mflix_comments",
      "`awards.wins`",
      "`awards.nominations`",
      "`imdb.rating`",
      "`imdb.votes`",
      "`tomatoes.viewer.rating`",
      "`tomatoes.viewer.numreviews`",
      "`tomatoes.critic.rating`",
      "`tomatoes.critic.numreviews`",
      "`tomatoes.rotten`",
      "`tomatoes.critic.meter`"
    )

    val columns_new = Map(
      "oid__id" -> "movie_id",
      "awards.wins" -> "awards_wins",
      "awards.nominations" -> "awards_nominations",
      "imdb.rating" -> "imdb_rating",
      "imdb.votes" -> "imdb_votes",
      "tomatoes.viewer.rating" -> "tomatoes_viewer_rating",
      "tomatoes.viewer.numreviews" -> "tomatoes_viewer_numreviews",
      "tomatoes.critic.rating" -> "tomatoes_critic_rating",
      "tomatoes.critic.numreviews" -> "tomatoes_critic_numreviews",
      "tomatoes.rotten" -> "tomatoes_rotten",
      "tomatoes.critic.meter" -> "tomatoes_critic_meter"
    )

    for (key <- columns_new.keys)
      result = result.withColumnRenamed(key, columns_new.get(key).get)

    result
  }
}
