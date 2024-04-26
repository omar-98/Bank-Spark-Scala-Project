package silver.pipeline

import com.SparkBank.Bank.core.domain.NotreData
import com.SparkBank.Bank.core.error.IOError
import com.SparkBank.Bank.core.service.ETLService
import org.apache.spark.sql.{DataFrame, Dataset}
import silver.domain.Address

case class AddressStep(
    service: ETLService,
    address: NotreData
) {

  import service.spark.implicits._

  lazy val addressDS: Either[IOError, Dataset[Address]] = service.sparkReader.readAsDataset[Address](
    address,
    service.runDate,
    service.dateFormat
  )

  def computeAddressMetrics(addressDs: Dataset[Address])(df: DataFrame): DataFrame =
    addressDs.toDF()
}
