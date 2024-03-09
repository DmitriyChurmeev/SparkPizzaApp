package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{count, row_number}

object PizzaApp {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.length != 1) {
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Pizza App")
      .getOrCreate()

    val pizzaOrderDf = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(0))

    import spark.implicits._

    /**
     * к сожалению получилось решение только через оконные функции,
     * буду признателен если подскажите другое возможное решение.
     * мне кажется можно обойтись и без него
     */
    val pizzaOrderStatisticDf = pizzaOrderDf
      .groupBy("order_type", "address_id")
      .agg(count("value") as "orders_total",
        count("address_id") as "orders_cnt"
      )
      .withColumn("row", row_number over Window.partitionBy("order_type").orderBy($"orders_cnt".desc)).where($"row" === 1)
      .drop("row")

    pizzaOrderStatisticDf.show

    pizzaOrderStatisticDf.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("spark-cluster/spark-data/out")

  }

}




