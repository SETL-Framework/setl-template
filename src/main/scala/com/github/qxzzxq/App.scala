package com.github.qxzzxq

import com.github.qxzzxq.entity.TestObject
import com.github.qxzzxq.factory.{AnotherFactory, MyFactory, TestObjectContainerFactory}
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
      .setConnector("myConnector", deliveryId = "myConnector")

    setl
      .newPipeline()
      .setInput(222)
      .benchmark(true) // to be disabled in the production environment
      .addStage[MyFactory]()
      .addStage[AnotherFactory]()
      .addStage[TestObjectContainerFactory]()
      .describe()
      .run()
      .showDiagram

  }
}
