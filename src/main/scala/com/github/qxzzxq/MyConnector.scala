package com.github.qxzzxq

import com.jcdecaux.setl.config.Conf
import com.jcdecaux.setl.internal.{CanDelete, CanPartition}
import com.jcdecaux.setl.storage.connector.ConnectorInterface
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class MyConnector extends ConnectorInterface with CanPartition with CanDelete {

  import spark.implicits._

  var name: String = ""
  override def setConf(conf: Conf): Unit = {
    this.name = conf.get("name", "")
  }

  override def setConfig(config: Config): Unit = this.setConf(Conf.fromConfig(config))

  override def partitionBy(columns: String*): MyConnector.this.type = this

  override def delete(query: String): Unit = log.debug("delete")

  override def read(): DataFrame = Seq(name).toDF("MyConnector")

  override def write(t: DataFrame, suffix: Option[String]): Unit = log.debug("write with suffix")

  override def write(t: DataFrame): Unit = log.debug("write")
}
