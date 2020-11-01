package com.example.spark

import org.apache.spark.SharedSparkContext
import org.scalatest.Assertions.assertResult
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.funsuite.AnyFunSuiteLike


class ApplicationTest extends AnyFlatSpecLike with SharedSparkContext {

  "JSON array of consumptions" should "be generated correctly" in {
    Application
  }

}

/*
class TestSharedSparkContext extends AnyFunSuiteLike with SharedSparkContext {

  val expectedResult = List(("a", 3),("b", 2),("c", 4))

  test("Word counts should be equal to expected") {
    verifyWordCount(Seq("c a a b a c b c c"))
  }

  def verifyWordCount(seq: Seq[String]): Unit = {
    //assertResult(expectedResult)(new WordCount().transform(sc.makeRDD(seq)).collect().toList)
  }
}
*/
