package cpslab.util

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

/**
 * doesn't support default value, leave the default value setting to the
 * user of this class
 */
class Configuration(private var _config: Config) extends Serializable {

  def this(path: String) = this(ConfigFactory.parseFile(new File(path)))

  /**
   * get a new configuration object with the properties at certain path of this Configuration
   * @param path the indicated path
   * @return newly created configuration
   */
  def generateConfigurationAtPath(path: String): Configuration = {
    new Configuration(_config.withOnlyPath(path))
  }

  def getBoolean(path: String): Boolean = _config.getBoolean(path)

  def getString(path: String): String = _config.getString(path)

  def getDouble(path: String): Double = _config.getDouble(path)

  def getInt(path: String): Int = _config.getInt(path)

  def containsPath(path: String): Boolean = _config.hasPath(path)

  def addEntry(path: String, value: String) {
    _config = _config.withValue(path, ConfigValueFactory.fromAnyRef(value))
  }
}