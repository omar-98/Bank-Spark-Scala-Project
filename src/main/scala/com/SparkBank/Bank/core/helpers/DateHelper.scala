package com.SparkBank.Bank.core.helpers

import org.joda.time.format.DateTimeFormat

object DateHelper {


  def switchDateFormat(givenDate:String,inputFormat:String,outputFormat:String): String =
    DateTimeFormat.forPattern(inputFormat)
      .withZoneUTC()
      .parseDateTime(givenDate)
      .toString(outputFormat)

}
