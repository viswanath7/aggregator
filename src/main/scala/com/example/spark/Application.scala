package com.example.spark

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object Application {

  private[this] val log = LoggerFactory getLogger this.getClass

  //FIXME: For a production ready-code, the following constants must carry a location in S3 bucket
  // such as s3n://<url-with-file-path> so that EMR can easily access them
  private[this] val SALES_DATA_FILE_LOCATION = "data/sales.csv"
  private[this] val CALENDAR_DATA_FILE_LOCATION = "data/calendar.csv"
  private[this] val PRODUCT_DATA_FILE_LOCATION = "data/product.csv"
  private[this] val STORE_DATA_FILE_LOCATION = "data/store.csv"

  case class Sales(saleId:Long, netSales:Double, salesUnits:Int, storeId:Int, dateId:Int, productId:Long)
  case class Calendar(dateId:Int, calendarDay:Int, calendarYear:Int , weekNumberOfSeason:Int)
  case class Product(productId:Long, division:String, gender:String, category:String)
  case class Store(storeId:Int, channel:String, country:String)

  case class Consumption(uniqueKey:String, division:String, gender:String, category:String, channel:String, year:Int,
                         netSales:Map[String, Double], salesUnits:Map[String, BigInt])

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


  def main(args: Array[String]): Unit = {

    log info "Initialising spark session ..."

    val sparkSession = SparkSession
      .builder
      .appName("SalesData")
      .master("local[*]") //TODO: Local; not production variant. Avoid specifying spark configuration as it can't be overridden.
      .getOrCreate()

    import sparkSession.implicits._

    lazy val dateIdToYearWeekNumberMapping: Map[Int, (Int, Int)] = {

      def calendarDataSet = {
        log debug "Reading calendar data set ..."
        val calendarSchema = new StructType()
          .add("dateId", IntegerType, nullable = false)
          .add("calendarDay", IntegerType, nullable = false)
          .add("calendarYear", IntegerType, nullable = false)
          .add("weekNumberOfSeason", IntegerType, nullable = false)
        sparkSession.read
          .option("header", "true")
          .option("sep", ",")
          .schema(calendarSchema)
          .csv(CALENDAR_DATA_FILE_LOCATION)
          .as[Calendar]
      }

      log debug "Computing the mapping between date identifier and pair of year & week number"
      // Note: supplied calendar.csv is in the ascending order of year's date for a single year
      // We can therefore safely compute the week number using zip function
      calendarDataSet
      .rdd
      .zipWithIndex()
      .map(pair => {
        val dateId = pair._1.dateId
        val year = pair._1.calendarYear
        val weekNumber = Math.ceil((pair._2 + 1) / 7) + 1
        (dateId, (year, weekNumber.toInt) )
      } )
      .collect()
      .toMap
    }
    /*
    log debug "Mapping between date identifier and year, week number pair"
    dateIdToYearWeekNumberMapping.foreach(entry => log debug s"${entry._1} -> ( ${entry._2._1} , ${entry._2._2} )")
    */
    // As the dataset is always small, it's safe to broadcast it
    val calendarBroadcast: Broadcast[Map[Int, (Int, Int)]] = sparkSession.sparkContext.broadcast(dateIdToYearWeekNumberMapping)
    val yearUserDefinedFunction = udf((dateId: Int) => calendarBroadcast.value.get(dateId).map(_._1))
    val weekNumberUserDefinedFunction = udf((dateId: Int) => calendarBroadcast.value.get(dateId).map(_._2))

    lazy val storeIdToChannelMapping: Map[Int, String] = {
      log debug "Computing the mapping between store identifier and channel"

      def storeDataSet = {
        val storeSchema = new StructType()
          .add("storeId", IntegerType, nullable = false)
          .add("channel", StringType, nullable = true)
          .add("country", StringType, nullable = true)
        sparkSession.read
          .option("header", "true")
          .option("sep", ",")
          .schema(storeSchema)
          .csv(STORE_DATA_FILE_LOCATION)
          .as[Store]
      }

      storeDataSet.select("storeId", "channel")
        .map( row => (row.getInt(0), row.getString(1)) )
        .collect()
        .toMap
    }
    /*
    log debug "Mapping between store identifier and channel"
    storeIdToChannelMapping.foreach(entry => log debug s"${entry._1} -> ${entry._2}")
    */
    // As the dataset is not expected to be massive, it's safe to broadcast it as one can fit it into memory
    val storeBroadcast = sparkSession.sparkContext.broadcast(storeIdToChannelMapping)
    val channelUserDefinedFunction = udf( (storeId:Int) => storeBroadcast.value.get(storeId) )

    lazy val salesDataSet = {
      val salesSchema = new StructType()
        .add("saleId", LongType, nullable = false)
        .add("netSales", DoubleType, nullable = true)
        .add("salesUnits", IntegerType, nullable = true)
        .add("storeId", IntegerType, nullable = false)
        .add("dateId", IntegerType, nullable = false)
        .add("productId", LongType, nullable = false)
      sparkSession.read
        .option("header", "true")
        .option("sep", ",")
        .schema(salesSchema)
        .csv(SALES_DATA_FILE_LOCATION)
        .as[Sales]
    }

    lazy val productDataSet = {
      val productSchema = new StructType()
        .add("productId", LongType, nullable = false)
        .add("division", StringType, nullable = true)
        .add("gender", StringType, nullable = true)
        .add("category", StringType, nullable = true)
      sparkSession.read
        .option("header", "true")
        .option("sep", ",")
        .schema(productSchema)
        .csv(PRODUCT_DATA_FILE_LOCATION)
        .as[Product]
    }



    // Use sales dataset per product
    val enhancedSalesData = salesDataSet
      .repartition(64) // Repartition dataframe before running large operation
      .withColumn("year", yearUserDefinedFunction(col("dateId")))
      .withColumn("weekNumber", weekNumberUserDefinedFunction(col("dateId")))
      .withColumn("channel", channelUserDefinedFunction(col("storeId")))
      .select("saleId", "netSales", "salesUnits", "storeId", "productId", "year", "weekNumber", "channel")
      .join( productDataSet, usingColumn = "productId" )
      .persist()

    log debug "--------------------------------------"
    log debug "         Enhanced sales data          "
    log debug "--------------------------------------"
    enhancedSalesData
      .show(enhancedSalesData.count().toInt, false)

    log debug "--------------------------------------"
    log debug "         Flattened sales data          "
    log debug "--------------------------------------"
    enhancedSalesData
      .groupBy("division", "gender", "category", "channel", "year", "weekNumber")
      .agg(round(sum("netSales"),2).alias("netSales"),
          round(sum("salesUnits"),2).alias("salesUnits"))
      .withColumn("uniqueKey",
        concat(
          col("year"), lit("_"), col("channel"), lit("_"),
          col("division"), lit("_"), col("gender"), lit("_"), col("category")
        )
      )
      .as[FlattenedSalesData]
      .map(_.toConsumption)
      .toJSON
      .show(numRows = 100, truncate = false)

/*

    log debug "--------------------------------------"
    log debug " Sales data grouped by week number    "
    log debug "--------------------------------------"
    enhancedSalesData
      .groupBy("weekNumber")
      .agg(round(sum("netSales"),2).alias("net_sales"),
        round(sum("salesUnits"),2).alias("sales_units"))
      .show(numRows = 100)

*/

  }

}