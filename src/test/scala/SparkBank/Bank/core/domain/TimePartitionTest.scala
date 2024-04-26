package SparkBank.Bank.core.domain
import com.SparkBank.Bank.core.SparkApplication
import com.SparkBank.Bank.core.domain.{DailyTimePartition, DataSource, NotreData}
import com.SparkBank.Bank.core.service.ETLService
import org.apache.spark.sql.DataFrame
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TimePartitionTest extends AnyFlatSpec with should.Matchers with PrivateMethodTester with SparkApplication {
  override val runDate = Some("20200406T145511.067Z")
  override val dateFormat = Some("yyyyMMdd'T'HHmmss.SSSZZ")
  val service = ETLService(spark, runDate.get, dateFormat.get)
  val dataSourceTest = DataSource(
    "datalake",
    "raw",
    "csv",
    "busnisUnitTEst",
    Some(Map("delimiter" -> ";",
      "header" -> "true")),
    None
  )
  val timePartition = DailyTimePartition("yyyy", "MM", "dd")
  val data = NotreData(dataSourceTest, "contact", timePartition)


      "time partition" should "return à path with specicique year and mont and day values" in
    {timePartition.getDataPath(data,runDate.get,dateFormat.get) shouldBe "datalake\\raw\\busnisUnitTEst\\contact\\year=2020\\month=04\\day=06"}

}