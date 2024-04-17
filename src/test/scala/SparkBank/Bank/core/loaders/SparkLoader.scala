package SparkBank.Bank.core.loaders

import org.apache.spark.sql.DataFrame
import com.SparkBank.Bank.core.domain.NotreData
import org.apache.spark.sql.SparkSession

case class SparkLoader(spark: SparkSession){
  def load(data: NotreData) : DataFrame= spark.read.format(data.dataFormat).load(data.dataSource.getPath)
}
