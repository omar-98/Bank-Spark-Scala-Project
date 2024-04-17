package SparkBank.Bank.core.domain

import com.SparkBank.Bank.core.domain.{DataSource, NotreData}

case class TestNotreData(dataSource:DataSource,dataFormat: String,dataName: String ) extends NotreData
