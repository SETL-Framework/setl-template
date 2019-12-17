package com.jcdecaux.datacorp

import com.jcdecaux.datacorp.entity.TestObject
import com.jcdecaux.datacorp.factory.MyFactory
import com.jcdecaux.setl.Setl

/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {

    val setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl
      .setSparkRepository[TestObject]("testObjectRepository")

    setl
      .newPipeline()
      .addStage[MyFactory]()
      .describe()
      .run()
  }
}
