package com.example.spark

import com.example.spark.Application.Consumption
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class ConsumptionTest extends AnyFlatSpecLike with Matchers {

  "Combine method of consumption" should "" in {
    val consumption = Consumption(
      uniqueKey = "2018_Digital_APPAREL_KIDS_CRICKET", division = "APPAREL",
      gender = "KIDS", category = "CRICKET", channel = "Digital", year = 2018,
      netSales = Map("W1"->15.0), salesUnits = Map("W1" -> 15)
    )
    val result = consumption.combine(Map("W2" -> 10.0), Map("W2" -> 10))
    result.netSales should have size 2
    result.netSales should contain ("W1"->15.0)
    result.netSales should contain ("W2" -> 10.0)
    result.salesUnits should have size 2
    result.salesUnits should contain ("W1" -> 15)
    result.salesUnits should contain ("W2" -> 10)
  }

}
