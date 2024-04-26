package silver.config

import com.SparkBank.Bank.core.domain.NotreData
import com.typesafe.config.ConfigFactory
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource}
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

case class AppConfig(
                      rawContactData: NotreData,
                      rawAddressData: NotreData,
                      silverContactData: NotreData
)
object AppConfig {
  def apply(filePath: String): AppConfig = {
    val reader                                  = ConfigFactory.load(filePath)
    implicit def productHint[A]: ProductHint[A] = ProductHint(ConfigFieldMapping(CamelCase, CamelCase))
    ConfigSource.fromConfig(reader).loadOrThrow[AppConfig]
  }
}
