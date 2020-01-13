package com.github.qxzzxq

import com.github.qxzzxq.entity.TestObject
import com.jcdecaux.setl.storage.connector.FileConnector
import org.scalatest.{FunSuite, Matchers}

class AppSuite extends FunSuite with Matchers {

  test("App should write 2 rows") {
    val app = App
    app.main(Array.empty)
    val repo = app.setl.getSparkRepository[TestObject]("testObjectRepository")

    assert(repo.findAll().count() === 2)
    repo.findAll().collect() should contain theSameElementsAs Array(TestObject(1, "a", "A", 1L), TestObject(2, "b", "B", 2L))

    repo.getConnector.asInstanceOf[FileConnector].delete()
  }

}
