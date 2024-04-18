package com.SparkBank.Bank.core.utils

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.sql.SparkSession



trait SparkApplication {
  val log = LogManager.getRootLogger
  log.setLevel(Level.WARN)
  implicit val spark: SparkSession=SparkSession
    .builder()
    .master("local")
    .getOrCreate()
  log.warn("Hello demo")

}
