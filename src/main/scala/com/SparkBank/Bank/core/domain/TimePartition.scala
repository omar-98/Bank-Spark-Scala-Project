package com.SparkBank.Bank.core.domain

import com.SparkBank.Bank.core.helpers.DateHelper

import java.nio.file.Paths



sealed trait TimePartition {
  def getDataPath(data: NotreData, gienDate: String, dateFormat: String): String
}

case class DailyTimePartition(
                             yearFormat:String,
                             monthFormat:String,
                             dayFormat:String
                             )extends TimePartition {
  def getDataPath(data:NotreData,givenDate:String,dateFormat:String):String=
    Paths.get(
      data.dataSource.sourceBasePath,
      data.dataSource.sourceType,
      data.dataSource.businessUnit,
      data.dataName,
      f"year=${DateHelper.switchDateFormat(givenDate,dateFormat,yearFormat)}",
      f"month=${DateHelper.switchDateFormat(givenDate, dateFormat, monthFormat)}",
      f"day=${DateHelper.switchDateFormat(givenDate, dateFormat, dayFormat)}"
    ).toString
}
