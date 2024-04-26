package com.SparkBank.Bank.core.error

trait IOError {
val exception: Exception
  val errorCode:String
}
case class FileNotFound(exception: Exception) extends IOError{
  override  val errorCode: String ="FileNotFound"
}

case class IOFailure(exception: Exception) extends IOError {
  override val errorCode: String = "UnhandledIOFailure"
}