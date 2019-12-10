package com.jcdecaux.datacorp

import com.jcdecaux.datacorp.entity.TestObject
import com.jcdecaux.datacorp.factory.MyFactory
import com.jcdecaux.datacorp.spark.DCContext

/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {

    val context = DCContext.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    println(context.configLoader.get("app.context.spark.spark.app.name"))

    context.setSparkRepository[TestObject]("testObjectRepository")

    context
      .newPipeline()
      .addStage[MyFactory]()
      .describe()
      .run()
  }
}
