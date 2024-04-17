package com.SparkBank.Bank.core.domain

trait DataSource {
  val businessUnit: String
  val backend: String
  def getPath: String
}

case class RawDataSource(businessUnit: String ,backend: String ) extends DataSource {
  override def getPath: String = "src/test/scala/SparkBank/Bank/data/raw"
}
case class SilverDataSource(businessUnit: String ,backend: String ) extends DataSource {
  override def getPath: String = "src/test/scala/SparkBank/Bank/data/silver"
}
case class GoldDataSource(businessUnit: String ,backend: String ) extends DataSource {
  override def getPath: String = "src/test/scala/SparkBank/Bank/data/gold"
}