package com.SparkBank.Bank.silver.detention.pipeline

import com.SparkBank.Bank.core.domain.NotreData
import com.SparkBank.Bank.core.transformers.FullJoin
import com.SparkBank.Bank.silver.detention.domain.{ CheckingAccount, InsuranceAccount, SecurityAccount}
import com.SparkBank.Bank.silver.detention.service.ETLService
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{ DataFrame, Dataset}


case class DetentionStep(service: ETLService, dataCheckingAccount: NotreData,dataAssuranceAccount:NotreData,dataSecurityAccount:NotreData){
  import service.spark.implicits._
  val checkingAccountData: DataFrame = service.sparkLoader.load(dataCheckingAccount)
  val insuranceAccountData: DataFrame = service.sparkLoader.load(dataAssuranceAccount)
  val securityAccountData: DataFrame = service.sparkLoader.load(dataSecurityAccount)

  def computeAccountTypeDf( aAccountType:String, aColumnsNameToSelect : Seq[String] )(df: DataFrame): DataFrame= {
    df.transform(addFixValueColumn("account_type", aAccountType))
      .transform(selectUsedColumns(aColumnsNameToSelect))
  }
  def computeDetentionDf(aChekingAccount: Dataset[CheckingAccount],aInsuranceAccount:Dataset[InsuranceAccount],aSecurityAccount:Dataset[SecurityAccount]): DataFrame={

    aChekingAccount.toDF
      .transform(computeAccountTypeDf("checking account",Seq("account_id","contact_id","account_type")))
      .transform(service.datasetTransformers.joinDataframes(aInsuranceAccount.toDF.transform(computeAccountTypeDf("inssurance account",Seq("account_id","contact_id","account_type"))),
      "account_id","account_id",FullJoin))
      .transform(service.datasetTransformers.joinDataframes(aSecurityAccount.toDF.transform (computeAccountTypeDf("security account", Seq("account_id", "contact_id", "account_type"))),
        "account_id","account_id",FullJoin))

  }

  private def addFixValueColumn(addedCoumnName:String,value :Any)(df:DataFrame) : DataFrame = df.withColumn(addedCoumnName, lit(value))
  private def selectUsedColumns(columnNameList: Seq[String])(df: DataFrame): DataFrame= df.select(columnNameList.head,columnNameList.tail:_*)
  }

