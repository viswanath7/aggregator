package com.example.spark

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
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

  def main(args: Array[String]): Unit = {

    implicit val sparkSession: SparkSession = {
      log debug "Initialising spark session ..."
      SparkSession
      .builder
      .appName("SalesData")
      .master("local[*]") //TODO: Local; not production variant. Avoid specifying spark configuration as it can't be overridden.
      .getOrCreate()
    }

    import sparkSession.implicits._
    def dateIdToYearWeekNumberMapping(fileLocation:String)(implicit sparkSession: SparkSession): Map[Int, (Int, Int)] = {

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
          .csv(fileLocation)
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

    // As the dataset is always small, it's safe to broadcast it
    val calendarBroadcast: Broadcast[Map[Int, (Int, Int)]] = sparkSession.sparkContext.broadcast(dateIdToYearWeekNumberMapping(CALENDAR_DATA_FILE_LOCATION))
    val yearUserDefinedFunction = udf((dateId: Int) => calendarBroadcast.value.get(dateId).map(_._1))
    val weekNumberUserDefinedFunction = udf((dateId: Int) => calendarBroadcast.value.get(dateId).map(_._2))

    def storeIdToChannelMapping(fileLocation:String)(implicit sparkSession: SparkSession): Map[Int, String] = {
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
          .csv(fileLocation)
          .as[Store]
      }

      storeDataSet.select("storeId", "channel")
        .map( row => (row.getInt(0), row.getString(1)) )
        .collect()
        .toMap
    }

    // As the dataset is not expected to be massive, it's safe to broadcast it as one can fit it into memory
    val storeBroadcast = sparkSession.sparkContext.broadcast(storeIdToChannelMapping(STORE_DATA_FILE_LOCATION))
    val channelUserDefinedFunction = udf( (storeId:Int) => storeBroadcast.value.get(storeId) )

    implicit def salesDataSet(implicit sparkSession: SparkSession): Dataset[Sales] = {
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

    implicit def productDataSet(implicit sparkSession: SparkSession): Dataset[Product] = {
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

    implicit def enhancedSalesDataSet(implicit salesDataset: Dataset[Sales], productDataset: Dataset[Product]): Dataset[EnhancedSales] = {
      salesDataset
      .repartition(64) // Repartition dataframe before running large operation
      .withColumn("year", yearUserDefinedFunction(col("dateId")))
      .withColumn("weekNumber", weekNumberUserDefinedFunction(col("dateId")))
      .withColumn("channel", channelUserDefinedFunction(col("storeId")))
      .select("saleId", "netSales", "salesUnits", "storeId", "productId", "year", "weekNumber", "channel")
      .join( productDataset, usingColumn = "productId" )
      .as[EnhancedSales]
      .persist()
    }

    log debug "---------- Enhanced sales data ----------"
    enhancedSalesDataSet.show(enhancedSalesDataSet.count().toInt, false)

    implicit def consumptions(implicit enhancedSales: Dataset[EnhancedSales]): Dataset[String] = {
      enhancedSales
      .groupBy("division", "gender", "category", "channel", "year", "weekNumber")
      .agg(round(sum("netSales"), 2).alias("netSales"),
        round(sum("salesUnits"), 2).alias("salesUnits"))
      .withColumn("uniqueKey",
        concat(
          col("year"), lit("_"), col("channel"), lit("_"),
          col("division"), lit("_"), col("gender"), lit("_"), col("category")
        ))
      .as[FlattenedSalesData]
      .map(_.toConsumption)
      .groupByKey(consumption => consumption.uniqueKey)
      .reduceGroups((first, second) => first.combine(second.netSales, second.salesUnits))
      .map(_._2)
      .toJSON
    }
    log debug "---------- Consumptions ----------"
    consumptions.show(numRows = consumptions.count().toInt, truncate = false)

  }

}