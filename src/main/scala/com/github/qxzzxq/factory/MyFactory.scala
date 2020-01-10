package com.github.qxzzxq.factory

import com.github.qxzzxq.entity.TestObject
import com.jcdecaux.setl.annotation.{Benchmark, Delivery}
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

@Benchmark
class MyFactory extends Factory[Dataset[TestObject]] with HasSparkSession {

  import spark.implicits._

  @Delivery
  private[this] val repo = SparkRepository[TestObject]

  private[this] var output = spark.emptyDataset[TestObject]

  override def read(): MyFactory.this.type = this

  @Benchmark
  override def process(): MyFactory.this.type = {
    output = Seq(TestObject(1, "a", "A", 1L), TestObject(2, "b", "B", 2L)).toDS()
    this
  }

  @Benchmark
  override def write(): MyFactory.this.type = {
    repo.save(output)
    this
  }

  override def get(): Dataset[TestObject] = output

}
