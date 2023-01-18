package libs.dqc

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

class Dqc {

  def doTesting (
    spark: SparkSession, dataset_s3_path: String, check_type: Check
  ): VerificationResult = {

    val data =
      spark.read.parquet (dataset_s3_path)
    val verificationResult = VerificationSuite ()
      .onData (data)
      .addCheck (
        check_type
      )
      .run ()

    verificationResult

  }

  def sendResultToSlack(slack_url: String,tb_name: String, verificationResult: VerificationResult): Unit = {
    AfterDqcProcess.setUpSlcakUrl(slack_url)
    AfterDqcProcess.chekVerificationResult(verificationResult, tb_name)
  }

}
