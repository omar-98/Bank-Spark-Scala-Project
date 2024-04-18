package com.SparkBank.Bank.silver.detention.pipeline
import com.SparkBank.Bank.silver.detention.domain.{CheckingAccount, InsuranceAccount, SecurityAccount}
import com.SparkBank.Bank.core.sink.Writer
import com.SparkBank.Bank.silver.detention.service.ETLService
import org.apache.spark.sql.SaveMode

case class SilverDetentionPipeline(service: ETLService,
                                   detentionStep: DetentionStep
  ) extends Writer {

  import service.spark.implicits._

  val silverDetention = detentionStep.computeDetentionDf(
    detentionStep.checkingAccountData.as[CheckingAccount],
    detentionStep.insuranceAccountData.as[InsuranceAccount],
    detentionStep.securityAccountData.as[SecurityAccount])


  writeParquet(silverDetention, "path", SaveMode.Overwrite, None)

}