package net.navinet.research.sparktest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pteixeira on 5/25/2016.
  */
object JoinTest {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")

    val sparkContext = new SparkContext(conf)
    val jsonSqlContext = new SQLContext(sparkContext)
    val postgresContext = new SQLContext(sparkContext)

    val jsonDataFrame = jsonSqlContext.read.json("./fake.json")
    val pgDataFrame = postgresContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:postgresql:postgres?user=postgres&password-",
        "dbtable" -> "public.people"
      )
    ).load()

    pgDataFrame
      .join(jsonDataFrame, pgDataFrame("id") === jsonDataFrame("id"))
      .show()
  }
}
