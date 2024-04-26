package com.SparkBank.Bank.core.domain

import com.SparkBank.Bank.core.domain.Data.DataPath

case class NotreData(
   dataSource:DataSource,
   dataName: String,
   timePartition: TimePartition) {
// or this
self =>

def getDataPath(givenDate: String, dateFormat: String): DataPath =
timePartition.getDataPath(self, givenDate, dateFormat)
}
object Data {
  type DataPath = String
}