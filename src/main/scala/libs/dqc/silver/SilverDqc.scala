package libs.dqc.silver

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.http.impl.client.DefaultHttpClient
import com.typesafe.config.{Config => TConfig}
import javax.net.ssl.HttpsURLConnection
import libs.etl.utils.AfterDqcProcess

object SilverDqc {

  def dqcTbGenre(
      spark: SparkSession,
      dataset_s3_path: String,
      slack_url: String,
      tb_name: String
  ): Unit = {
    val data =
      spark.read.parquet(dataset_s3_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "mflix tb_genre")
          .isComplete("movie_id")
          .hasMaxLength("movie_id", _ == 24.0)
      )
      .run()

    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }

  def dqcTbCast(
      spark: SparkSession,
      dataset_s3_path: String,
      slack_url: String,
      tb_name: String
  ): Unit = {
    val data =
      spark.read.parquet(dataset_s3_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "mflix tb_cast")
          .isComplete("movie_id")
          .hasMaxLength("movie_id", _ == 24.0)
      )
      .run()

    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }

  def dqcTbCountries(
      spark: SparkSession,
      dataset_s3_path: String,
      slack_url: String,
      tb_name: String
  ): Unit = {
    val data =
      spark.read.parquet(dataset_s3_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "mflix tb_countries")
          .isComplete("movie_id")
          .hasMaxLength("movie_id", _ == 24.0)
      )
      .run()

    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }

  def dqcTbDirectors(
      spark: SparkSession,
      dataset_s3_path: String,
      slack_url: String,
      tb_name: String
  ): Unit = {
    val data =
      spark.read.parquet(dataset_s3_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "mflix tb_directors")
          .isComplete("movie_id")
          .hasMaxLength("movie_id", _ == 24.0)
      )
      .run()

    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }

  def dqcTbLanguages(
      spark: SparkSession,
      dataset_s3_path: String,
      slack_url: String,
      tb_name: String
  ): Unit = {
    val data =
      spark.read.parquet(dataset_s3_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "mflix tb_directors")
          .isComplete("movie_id")
          .hasMaxLength("movie_id", _ == 24.0)
      )
      .run()

    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }

  def dqcTbWriters(
      spark: SparkSession,
      dataset_s3_path: String,
      slack_url: String,
      tb_name: String
  ): Unit = {
    val data =
      spark.read.parquet(dataset_s3_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "mflix tb_directors")
          .isComplete("movie_id")
          .hasMaxLength("movie_id", _ == 24.0)
      )
      .run()

    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }

  def dqcTbMovieFacts(
      spark: SparkSession,
      dataset_s3_path: String,
      slack_url: String,
      tb_name: String
  ): Unit = {
    val data =
      spark.read.parquet(dataset_s3_path)
    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "mflix tb_directors")
          .isComplete("movie_id")
          .hasMaxLength("movie_id", _ == 24.0)
          .isComplete("num_mflix_comments")
      )
      .run()

    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }
}
