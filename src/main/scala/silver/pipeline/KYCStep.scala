package silver.pipeline

import com.SparkBank.Bank.core.domain.NotreData
import com.SparkBank.Bank.core.error.IOError
import com.SparkBank.Bank.core.service.ETLService
import org.apache.spark.sql.{DataFrame, Dataset}
import silver.domain.Contact

case class KYCStep(
    service: ETLService,
    contact: NotreData
) {
  import service.spark.implicits._

  lazy val contactDS: Either[IOError, Dataset[Contact]] = service.sparkReader.readAsDataset[Contact](
    contact,
    service.runDate,
    service.dateFormat
  )

  def computeKycMetrics(contactDs: Dataset[Contact]): DataFrame =
    contactDs.toDF()
}
