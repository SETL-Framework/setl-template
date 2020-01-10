package com.github.qxzzxq.factory

import com.github.qxzzxq.entity.{TestObject, TestObjectContainer}
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class TestObjectContainerFactory extends Factory[Dataset[TestObjectContainer]] with HasSparkSession {

  import spark.implicits._

  @Delivery // this dataset will be delivered by the SETL
  private[this] val testObject = spark.emptyDataset[TestObject]

  private[this] var output = spark.emptyDataset[TestObjectContainer]

  override def read(): TestObjectContainerFactory.this.type = {
    this
  }

  override def process(): TestObjectContainerFactory.this.type = {
    output = Seq(TestObjectContainer("id", testObject.collect())).toDS()
    this
  }

  override def write(): TestObjectContainerFactory.this.type = {
    output.show(false)
    this
  }

  override def get(): Dataset[TestObjectContainer] = output
}
