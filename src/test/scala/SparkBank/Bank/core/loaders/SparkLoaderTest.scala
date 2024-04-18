package SparkBank.Bank.core.loaders.S
import SparkBank.Bank.core.domain.TestNotreData
import com.SparkBank.Bank.core.domain.RawDataSource
import com.SparkBank.Bank.core.loaders.SparkLoader
import com.SparkBank.Bank.core.utils.SparkApplication
import org.apache.spark.sql.DataFrame
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SparkLoaderTest extends AnyFlatSpec with should.Matchers with PrivateMethodTester with SparkApplication{
    val rawDataSource =RawDataSource("businessUnnitTest","backendTest")
    val data= TestNotreData(rawDataSource,"csv","test_load_data")
    val sparkLoader= SparkLoader(spark)
    sparkLoader.load(data).show()

    "a SparkLoader" should "return a csv file" in
    {sparkLoader.load(data).count() shouldBe 2}
}
