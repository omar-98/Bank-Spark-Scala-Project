package com.SparkBank.Bank.silver.contact.pipeline
import com.SparkBank.Bank.core.sink.Writer
import com.SparkBank.Bank.silver.contact.domain.Contact
import com.SparkBank.Bank.silver.contact.service.ETLService
import org.apache.spark.sql.SaveMode

case class SilverContactPipeline(
                                service:ETLService,
                                kycStep: KycStep
                                )extends Writer{

  import service.spark.implicits._
  val silverContact =kycStep.computeKycMetrics(kycStep.contact.as[Contact])

  writeParquet(silverContact,"path",SaveMode.Overwrite,None)

}
