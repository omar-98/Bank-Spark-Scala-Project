package SparkBank.Bank.core.transformers

import com.SparkBank.Bank.core.transformers.{DatasetTransformers, LeftJoin, RightJoin}
import com.SparkBank.Bank.core.utils.SparkApplication
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.spark.sql.functions.{max, min}
class DatasetTransformersTest extends AnyFlatSpec with should.Matchers with PrivateMethodTester with SparkApplication {
import spark.implicits._
  val datasetTransformers= DatasetTransformers(spark,"toto")
  val multipleAggregationInputDf = spark.range(5).withColumn("group", $"id" % 2)
  val findFirstAndSecondBestSellerBygenreInput = Seq(
    (1, "Hunter Fields", "romance", 15),
    (2, "Leonard Lewis", "thriller", 81),
    (3, "Jason Dawson", "thriller", 90),
    (4, "Andre Grant", "thriller", 25),
    (5, "Earl Walton", "romance", 40),
    (6, "Alan Hanson", "romance", 24),
    (7, "Clyde Matthews", "thriller", 31),
    (8, "Josephine Leonard", "thriller", 1),
    (9, "Owen Boone", "sci-fi", 27),
    (10, "Max McBride", "romance", 75),
  ).toDF("id", "title", "genre", "quantity")

 //multipleAggregationInputDf.show()
  val groupByColumns= Seq($"group")
  val aggSpec = Map(
    "max_id" -> max("id"),
    "min_id" -> min("id")
  )
  log.warn("Test 1")
  "a doMultipleAggregation" should "return int value equals to 5" in {
    datasetTransformers.doMultipleAggregation(groupByColumns, aggSpec)(multipleAggregationInputDf).take(1).toSeq should contain theSameElementsAs Seq(0, 4, 0)
  }
    log.warn("Test 2")
  "a joinDataframes " should "return value equals to 5" in {
    datasetTransformers.joinDataframes(findFirstAndSecondBestSellerBygenreInput,"id","id", LeftJoin)(multipleAggregationInputDf).count shouldBe  5
  }
    log.warn("Test 3")
  "a joinDataframes " should "return value equals to 10" in {
    datasetTransformers.joinDataframes(findFirstAndSecondBestSellerBygenreInput, "id", "id", RightJoin)(multipleAggregationInputDf).count shouldBe 10
  }
  log.warn("I am done")
}


