package com.SparkBank.Bank.silver.contact.pipeline

import com.SparkBank.Bank.core.domain.NotreData
import com.SparkBank.Bank.silver.contact.domain.Contact
import com.SparkBank.Bank.silver.contact.service.ETLService
import org.apache.spark.sql.functions.{col, current_date, months_between, round}
import org.apache.spark.sql.{DataFrame, Dataset}
case class KycStep(service: ETLService, dataContact: NotreData){



  val contact: DataFrame = service.sparkLoader.load(dataContact)
  def computeKycMetrics(contact: Dataset[Contact]): DataFrame = {
    contact.toDF
      .transform(computeAgeFromDob("date_of_birth"))
      .transform(computeAnciente("join_date"))
  }
  private def computeAgeFromDob(dateOfBirthColumnName:String)(df: DataFrame) : DataFrame= {
    df.withColumn("age", round(months_between(current_date(), col(dateOfBirthColumnName)), 0))

  }
  private def computeAnciente(joinDateColumnName:String)(df: DataFrame): DataFrame = {
    df.withColumn("anciente", round(months_between(current_date(), col(joinDateColumnName)), 0))
  }

}
