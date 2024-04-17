package SparkBank.Bank.core.transformers

import com.SparkBank.Bank.core.SparkApplication
import com.SparkBank.Bank.core.transformers.DatasetTransformers
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.spark.sql.functions.{min,max}
class DatasetTransformersTest extends AnyFlatSpec with should.Matchers with PrivateMethodTester with SparkApplication {
import spark.implicits._
  val datasetTransformers= DatasetTransformers(spark,"toto")
  val multipleAggregationInputDf = spark.range(5).withColumn("group", $"id" % 2)
 // println(multipleAggregationInputDf.take(1))
  val groupByColumns= Seq($"group")
  val aggSpec = Map(
    "max_id" -> max("id"),
    "min_id" -> min("id")
  )
  "a doMultipleAggregation" should "return int value equals to 4"
  datasetTransformers.doMultipleAggregation(groupByColumns, aggSpec)(multipleAggregationInputDf).take(1)(0)(1) shouldBe 4

}


