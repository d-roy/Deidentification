package net.navinet.research.sparktest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pteixeira on 6/2/2016.
  */
object SumTest {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("TestSum")
      .setMaster("local")

    val sparkContext = new SparkContext(conf)
    val jsonSqlContext = new SQLContext(sparkContext)

    sparkContext.addFile(args(0))

    val jsonDataFrame = jsonSqlContext
      .read
      .format("json")
      .option("path", args(0))
      .option("dbtable", "People")
      .load()

    jsonDataFrame
      .withColumn("sum", jsonDataFrame("number1") + jsonDataFrame("number2"))
      .show()

    sparkContext.stop()
  }
}
