package com.github.qxzzxq.factory

import com.github.qxzzxq.entity.TestObject
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession

class AnotherFactory extends Factory[String] with HasSparkSession {

  import spark.implicits._

  @Delivery
  private[this] val outputOfMyFactory = spark.emptyDataset[TestObject]

  @Delivery(id = "myConnector")
  private[this] val connector = Connector.empty

  override def read(): AnotherFactory.this.type = this

  override def process(): AnotherFactory.this.type = this

  override def write(): AnotherFactory.this.type = {
    outputOfMyFactory.show()
    connector.read().show()
    this
  }

  override def get(): String = "output"
}
