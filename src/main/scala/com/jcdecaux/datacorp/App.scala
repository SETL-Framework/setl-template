package com.jcdecaux.datacorp

import com.jcdecaux.datacorp.spark.DCContext

/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {

    val context = DCContext
      .builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    println(context.configLoader.get("app.general.conf"))
    println(context.configLoader.get("app.env-specific.conf"))

    context.newPipeline().describe().run()

  }
}
