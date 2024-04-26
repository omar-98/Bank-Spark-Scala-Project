package com.SparkBank.Bank.core.domain

case class DataSource(
  sourceBasePath: String, // /datalake for example
  sourceType: String, // raw, silver or gold
  sourceFormat: String, //csv, parquet, etc
  businessUnit: String,
  readOptions: Option[Map[String, String]],
  writeOptions: Option[Map[String, String]])
