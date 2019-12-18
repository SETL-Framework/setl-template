package com.jcdecaux.datacorp.factory

import com.jcdecaux.datacorp.entity.TestObject
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class MyFactory extends Factory[Dataset[TestObject]] with HasSparkSession {

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
