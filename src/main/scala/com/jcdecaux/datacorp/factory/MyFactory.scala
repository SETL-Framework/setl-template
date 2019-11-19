package com.jcdecaux.datacorp.factory

import com.jcdecaux.datacorp.entity.TestObject
import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.storage.repository.SparkRepository
import com.jcdecaux.datacorp.spark.transformation.Factory
import org.apache.spark.sql.{Dataset, SparkSession}

class MyFactory extends Factory[Dataset[TestObject]] {

  // Retrieve the active spark session
  val spark: SparkSession = SparkSession.getActiveSession.get

  @Delivery
  var repo: SparkRepository[TestObject] = _
  var output: Dataset[TestObject] = _

  override def read(): MyFactory.this.type = this

  override def process(): MyFactory.this.type = {
    import spark.implicits._
    output = Seq(TestObject(1, "a", "A", 1L), TestObject(2, "b", "B", 2L)).toDS()
    this
  }

  override def write(): MyFactory.this.type = {
    repo.save(output)
    this
  }

  override def get(): Dataset[TestObject] = output

}
