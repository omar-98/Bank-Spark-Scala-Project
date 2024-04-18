package com.SparkBank.Bank.silver.encours.pipeline



import com.SparkBank.Bank.core.domain.NotreData
import com.SparkBank.Bank.core.transformers.FullJoin
import com.SparkBank.Bank.silver.encours.domain.{ EncoursChecking, EncoursSecurity}
import com.SparkBank.Bank.silver.encours.service.ETLService
import org.apache.spark.sql.{ DataFrame, Dataset}


case class EncoursStep(service: ETLService, dataCheckingEncours: NotreData,dataSecurityEncours:NotreData){
  import service.spark.implicits._
  val checkingEncoursData: DataFrame = service.sparkLoader.load(dataCheckingEncours)
  val securityEncoursData: DataFrame = service.sparkLoader.load(dataSecurityEncours)

def computeEncoursTotale(aEncoursChecking:Dataset[EncoursChecking],aEncoursSecurity: Dataset[EncoursSecurity]): DataFrame={
  aEncoursChecking.toDF
    .transform(service.datasetTransformers.joinDataframes(aEncoursSecurity.toDF
    .transform( data => data.withColumnRenamed("account_balance","Security_balance")),"contact_id","contact id", FullJoin))
    .transform(service.datasetTransformers.selectMutipleColumns(List("contact_id","account_id","security_balance","checking_balance")))
}

}




