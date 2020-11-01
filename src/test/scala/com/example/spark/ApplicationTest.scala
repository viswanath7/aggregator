package com.example.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
class ApplicationTest extends AnyFlatSpecLike with Matchers with SharedSparkContext {

  "JSON array of consumptions" should "be generated correctly" in {
    implicit val sparkSession: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    import sparkSession.implicits._
    val enhancedSales = Seq(
      EnhancedSales(565177969035L, 128, 400.24, 5, 409, 2018, 1, "Digital", "Footwear", "Mens", "Collections"),
      EnhancedSales(565177969059L, 1113, 360.24, 5, 409, 2018, 21, "Digital", "Apparel", "Kids", "Collections")
    )
    val firstJSON = """{"uniqueKey":"2018_Digital_Apparel_Kids_Collections","division":"Apparel","gender":"Kids","category":"Collections","channel":"Digital","year":2018,"netSales":{"W15":0.0,"W26":0.0,"W37":0.0,"W33":0.0,"W36":0.0,"W48":0.0,"W30":0.0,"W12":0.0,"W23":0.0,"W45":0.0,"W5":0.0,"W25":0.0,"W42":0.0,"W16":0.0,"W27":0.0,"W41":0.0,"W19":0.0,"W38":0.0,"W49":0.0,"W11":0.0,"W4":0.0,"W3":0.0,"W52":0.0,"W44":0.0,"W22":0.0,"W40":0.0,"W46":0.0,"W8":0.0,"W21":360.24,"W32":0.0,"W10":0.0,"W50":0.0,"W28":0.0,"W7":0.0,"W17":0.0,"W51":0.0,"W39":0.0,"W2":0.0,"W47":0.0,"W20":0.0,"W14":0.0,"W31":0.0,"W34":0.0,"W35":0.0,"W6":0.0,"W1":0.0,"W29":0.0,"W24":0.0,"W43":0.0,"W53":0.0,"W9":0.0,"W13":0.0,"W18":0.0},"salesUnits":{"W15":0,"W26":0,"W37":0,"W33":0,"W36":0,"W48":0,"W30":0,"W12":0,"W23":0,"W45":0,"W5":0,"W25":0,"W42":0,"W16":0,"W27":0,"W41":0,"W19":0,"W38":0,"W49":0,"W11":0,"W4":0,"W3":0,"W52":0,"W44":0,"W22":0,"W40":0,"W46":0,"W8":0,"W21":5,"W32":0,"W10":0,"W50":0,"W28":0,"W7":0,"W17":0,"W51":0,"W39":0,"W2":0,"W47":0,"W20":0,"W14":0,"W31":0,"W34":0,"W35":0,"W6":0,"W1":0,"W29":0,"W24":0,"W43":0,"W53":0,"W9":0,"W13":0,"W18":0}}"""
    val secondJSON = """{"uniqueKey":"2018_Digital_Footwear_Mens_Collections","division":"Footwear","gender":"Mens","category":"Collections","channel":"Digital","year":2018,"netSales":{"W15":0.0,"W26":0.0,"W37":0.0,"W33":0.0,"W36":0.0,"W48":0.0,"W30":0.0,"W12":0.0,"W23":0.0,"W45":0.0,"W5":0.0,"W25":0.0,"W42":0.0,"W16":0.0,"W27":0.0,"W41":0.0,"W19":0.0,"W38":0.0,"W49":0.0,"W11":0.0,"W4":0.0,"W3":0.0,"W52":0.0,"W44":0.0,"W22":0.0,"W40":0.0,"W46":0.0,"W8":0.0,"W21":0.0,"W32":0.0,"W10":0.0,"W50":0.0,"W28":0.0,"W7":0.0,"W17":0.0,"W51":0.0,"W39":0.0,"W2":0.0,"W47":0.0,"W20":0.0,"W14":0.0,"W31":0.0,"W34":0.0,"W35":0.0,"W6":0.0,"W1":400.24,"W29":0.0,"W24":0.0,"W43":0.0,"W53":0.0,"W9":0.0,"W13":0.0,"W18":0.0},"salesUnits":{"W15":0,"W26":0,"W37":0,"W33":0,"W36":0,"W48":0,"W30":0,"W12":0,"W23":0,"W45":0,"W5":0,"W25":0,"W42":0,"W16":0,"W27":0,"W41":0,"W19":0,"W38":0,"W49":0,"W11":0,"W4":0,"W3":0,"W52":0,"W44":0,"W22":0,"W40":0,"W46":0,"W8":0,"W21":0,"W32":0,"W10":0,"W50":0,"W28":0,"W7":0,"W17":0,"W51":0,"W39":0,"W2":0,"W47":0,"W20":0,"W14":0,"W31":0,"W34":0,"W35":0,"W6":0,"W1":5,"W29":0,"W24":0,"W43":0,"W53":0,"W9":0,"W13":0,"W18":0}}"""
    val actualResult = Application.consumptions(sc.makeRDD(enhancedSales).toDS()).collectAsList().asScala.toList
    actualResult should have size(2)
    actualResult should contain allOf(firstJSON,secondJSON)
  }

  "Sales data set" should "be loaded correctly" in {
    implicit val sparkSession: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    val products = Application.productDataSet("src/test/resources/small-products-list.csv").collectAsList().asScala.toList
    products should have size(2)
    products should contain allOf( Product(567228914507L,"APPAREL","KIDS","CRICKET"),
      Product(565177969035L,"FOOTWEAR","MENS","COLLECTIONS") )
  }
}
