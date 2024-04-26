package com.SparkBank.Bank.core

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession



trait SparkApplication extends App {
  val log = LogManager.getRootLogger
  log.setLevel(Level.WARN)
  implicit val spark: SparkSession=SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  protected val params: Map[String, String] = (for {
    arg <- args
    arr = arg.split("=") match {
      case Array(k, v) => Array(k, v)
      case Array(k) => Array(k, "")
    }
  } yield (arr(0) -> arr(1))).toMap

  protected val runDate = params.get("RUN_DATE")
  protected val dateFormat = params.get("DATE_FORMAT")
}
