package com.SparkBank.Bank.core

import org.apache.spark.sql.SparkSession

trait SparkApplication {
  val spark: SparkSession=SparkSession
    .builder()
    .master("local")
    .getOrCreate()

}
