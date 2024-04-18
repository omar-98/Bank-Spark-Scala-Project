package com.SparkBank.Bank.silver.detention.service

import com.SparkBank.Bank.core.loaders.SparkLoader
import com.SparkBank.Bank.core.transformers.DatasetTransformers
import org.apache.spark.sql.SparkSession

case class ETLService(spark : SparkSession) {
  val datasetTransformers=DatasetTransformers(spark,"toto")
  val sparkLoader =SparkLoader(spark)

}
