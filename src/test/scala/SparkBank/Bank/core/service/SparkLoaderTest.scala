package SparkBank.Bank.core.service.S
import com.SparkBank.Bank.core.SparkApplication
import com.SparkBank.Bank.core.domain.{DailyTimePartition, DataSource, NotreData}
import com.SparkBank.Bank.core.service.ETLService
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import org.scalatest.matchers.should

class SparkLoaderTest extends AnyFlatSpec with should.Matchers with PrivateMethodTester with SparkApplication{
  override val runDate = Some("20200406T145511.067Z")
  override val dateFormat = Some("yyyyMMdd'T'HHmmss.SSSZZ")
  val service = ETLService(spark, runDate.get, dateFormat.get)
    val dataSourceTest=DataSource(
      "datalake",
      "raw",
      "csv",
      "busnisUnitTest",
      Some(Map("delimiter"->",",
                  "header"->"true")),
      None
    )
  val timePartition=DailyTimePartition("yyyy","MM","dd")
    val data= NotreData(dataSourceTest,"contact",timePartition)


  "a SparkLoader" should "return a csv file" in
    {service.sparkReader.readAsDataFrame(data,runDate.get,dateFormat.get) should matchPattern { case Left(e) => }
}}
