package silver.pipeline

import com.typesafe.config._

object AppConfigTest extends App {

  println("_" * 50)
  val config: Config=ConfigFactory.load()
  println(config.getString("key1"))
  println("_" * 50)
}