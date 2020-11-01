package com.example

package object spark {

  case class Sales(saleId:Long, netSales:Double, salesUnits:Int, storeId:Int, dateId:Int, productId:Long)
  case class Calendar(dateId:Int, calendarDay:Int, calendarYear:Int , weekNumberOfSeason:Int)
  case class Product(productId:Long, division:String, gender:String, category:String)
  case class Store(storeId:Int, channel:String, country:String)

  case class Consumption(uniqueKey:String, division:String, gender:String, category:String, channel:String, year:Int,
                         netSales:Map[String, Double], salesUnits:Map[String, BigInt]) {
    def combine(netSales:Map[String, Double], salesUnits:Map[String, BigInt]): Consumption = {
      this.copy(netSales = this.netSales ++ netSales.filter(entry=> entry._2!=0), salesUnits = this.salesUnits ++ salesUnits.filter(entry=>entry._2!=0))
    }
  }

  case class FlattenedSalesData(uniqueKey:String, division:String, gender:String, category:String, channel:String, year:Int, weekNumber:Int,
                                netSales:Double, salesUnits:BigInt) {
    def toConsumption:Consumption = {
      Consumption(uniqueKey, division, gender, category, channel, year,
        netSales =  {
          val weekNumber = 1 to 53
          weekNumber
            .map(num => s"W$num")
            .map(key => (key, 0.0))
            .toMap
        } + (s"W$weekNumber" -> netSales),
        salesUnits =  {
          val weekNumber = 1 to 53
          weekNumber
            .map(num => s"W$num")
            .map(key => (key, BigInt(0) ))
            .toMap
        } + (s"W$weekNumber"->salesUnits))
    }
  }

}
