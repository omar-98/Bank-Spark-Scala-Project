package silver

import com.SparkBank.Bank.core.SparkApplication
import com.SparkBank.Bank.core.service.ETLService
import silver.config.AppConfig
import silver.pipeline.{AddressStep, KYCStep, SilverContactPipeline}

object SilverContact extends SparkApplication {

  val appConfig = AppConfig("application.conf")

  val service     = ETLService(spark, runDate.get, dateFormat.get)
  val kycStep     = KYCStep(service, appConfig.rawContactData)
  val addressStep = AddressStep(service, appConfig.rawAddressData)

  val silverContactPipeline = SilverContactPipeline(
    service,
    addressStep,
    kycStep
  )

  service.sparkWriter.write(
    appConfig.silverContactData,
    silverContactPipeline.computeSilverContact,
    runDate.get,
    dateFormat.get
  )

  spark.stop()

}
