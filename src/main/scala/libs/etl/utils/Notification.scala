package libs.etl.utils

import java.net.{http, URI}
import com.typesafe.config.{Config => TConfig}
import scalaj.http.{Http, HttpOptions}
import ujson.{Obj => JObject}

object Notification {
  def toSlack(
      message: String,
      url: String,
  ): Unit = {


    val r = requests.post(url, data = JObject("text" -> message))
    println(r.text())

  }
}