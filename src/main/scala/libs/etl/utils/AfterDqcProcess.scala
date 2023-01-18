package libs.etl.utils

import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

object AfterDqcProcess {
  val message_success = "The data passed the test, everything is fine!"
  var slack_url = ""

  def setUpSlcakUrl(slack_url_params: String): Unit = {
    this.slack_url = slack_url_params
  }

  def chekVerificationResult(
      verificationResult: VerificationResult,
      tb_name: String
  ): Unit = {

    val notification = Notification

    if (verificationResult.status == CheckStatus.Success) {
      val message = message_success+" in "+tb_name
      val title = "success from dqyc"

      notification.toSlack(
        message,
        slack_url
      )
    } else {
      println("We found errors in  "+tb_name+" :\n")

      var message = "found errors in  "+tb_name+" =======> "
      val title = "Data Quality Check Has Some Failures"

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          val temp_message = s"\\n${result.constraint}: ${result.message.get}"

          message = message + temp_message
          message =
            message + "\\n=============================================================="
        }

      notification.toSlack(
        message,
        slack_url
      )

      if (message != message_success) {
        println(message)
        System.exit(1)
      }

    }
  }

  def showResult(verificationResult: VerificationResult): Unit = {
    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          println(s"${result.constraint}: ${result.message.get}")
        }
    }
  }
}
