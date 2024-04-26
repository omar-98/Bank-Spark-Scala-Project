package com.SparkBank.Bank.core.io

import com.SparkBank.Bank.core.domain.NotreData
import com.SparkBank.Bank.core.error.{FileNotFound, IOError, IOFailure}
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Encoder, SparkSession}

import scala.util.{Failure, Success, Try}

case class SparkReaderAdapter(sparkSession: SparkSession) {
  import sparkSession.implicits._

  def readAsDataFrame(data: NotreData, givenDate: String, dateFormat: String): Either[DataFrame,IOError] = {
    val readOptions = data.dataSource.readOptions match {
      case None           => Map.empty[String, String]
      case Some(readOpts) => readOpts
    }

    Try(
      sparkSession.read
        .format(data.dataSource.sourceFormat)
        .options(readOptions)
        .load(data.getDataPath(givenDate, dateFormat))
    ) match {
      case Success(df) => Left(df)
      case Failure(exception: AnalysisException) =>
        if (exception.getMessage.toLowerCase contains "not exist") Right(FileNotFound(exception))
        else Right(IOFailure(exception))
      case Failure(exception: Exception) => Right(IOFailure(exception))
      case Failure(exception)            => throw exception
    }
  }

  def readAsDataset[Domain](data: NotreData, givenDate: String, dateFormat: String)(
      implicit encoder: Encoder[Domain]
  ): Either[IOError, Dataset[Domain]] =
    readAsDataFrame(data, givenDate, dateFormat) match {
      case Right(ioError) => Left(ioError)
      case Left(df)     => Right(df.as[Domain].map(identity))
    }
}
