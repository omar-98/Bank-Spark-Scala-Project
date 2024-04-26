package silver.pipeline

import com.SparkBank.Bank.core.service.ETLService
import org.apache.spark.sql.DataFrame

case class SilverContactPipeline(
    service: ETLService,
    addressStep: AddressStep,
    KYCStep: KYCStep
) {
  def computeSilverContact: DataFrame = ???
}
