package com.github.qxzzxq

import com.github.qxzzxq.entity.TestObject
import com.github.qxzzxq.factory.{MyFactory, TestObjectContainerFactory}
import com.jcdecaux.setl.Setl

/**
 * Hello world!
 *
 */
object App {

  val setl: Setl = Setl.builder()
    .withDefaultConfigLoader()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    setl
      .setSparkRepository[TestObject]("testObjectRepository")

    setl
      .newPipeline()
      .benchmark(true) // to be disabled in the production environment
      .addStage[MyFactory]()
      .addStage[TestObjectContainerFactory]()
      .describe()
      .run()
  }
}
