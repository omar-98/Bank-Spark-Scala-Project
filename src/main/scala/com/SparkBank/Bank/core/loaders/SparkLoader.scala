package com.SparkBank.Bank.core.loaders
import org.apache.spark.sql.DataFrame
import com.SparkBank.Bank.core.domain.NotreData
import org.apache.spark.sql.SparkSession

case class SparkLoader(spark: SparkSession){
  def load(data: NotreData) : DataFrame= {
    if (data.dataFormat == "csv")  spark.read.format(data.dataFormat).option("header",true).option("delimiter",";").load(data.dataSource.getPath + "/" + data.dataName + "." + data.dataFormat)
    else spark.read.format(data.dataFormat).load(data.dataSource.getPath + "/" + data.dataName + "." + data.dataFormat)
  }
}
