package com.SparkBank.Bank.core.transformers

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class DatasetTransformers(spark: SparkSession,toto:String){
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
  def doMultipleAggregation(groupByColumns: Seq[Column],aggSpec: Map[String,Column])(df:DataFrame): DataFrame= {
    val aggregationSeq: Seq[Column] =aggSpec.map(singleAggregation =>{
      val outputColName =singleAggregation._1
      val aggregationExpresion = singleAggregation._2
      aggregationExpresion.as(outputColName)
    }).toSeq
    log.warn("group by" + groupByColumns.toString())
    df.groupBy(groupByColumns: _*).agg(aggregationSeq.head,aggregationSeq.tail: _*)

  }

  def selectMutipleColumns(columnsList: List[String])(df: DataFrame): DataFrame = df.select(columnsList.head,columnsList.tail:_*)

  def joinDataframes(joinedDataframe:DataFrame,leftjoinedColumnName:String,rightjoinedColumnName:String, typeOfjoin:JoinType)(df: DataFrame): DataFrame = {
    df.join(joinedDataframe, df(leftjoinedColumnName) ===joinedDataframe(rightjoinedColumnName),typeOfjoin.value)
  }
}
