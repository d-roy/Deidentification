package net.navinet.research.sparktest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pteixeira on 6/7/2016.
  */
object JoinDistributedTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Test Distributed Join")

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    val patient = sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://10.7.112.20:3306/COS_DB")
      .option("dbtable", "COS_PATIENT")
      .option("user", "cosuser")
      .option("password", "scarwood")
      .load()

    val encounter = sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://10.7.112.20:3306/COS_DB")
      .option("dbtable", "COS_PATIENT_ENCOUNTER")
      .option("user", "cosuser")
      .option("password", "scarwood")
      .load()

    val person = sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://10.7.112.20:3306/COS_DB")
      .option("dbtable", "COS_PERSON")
      .option("user", "cosuser")
      .option("password", "scarwood")
      .load()


    //    person
    //      .join(patient, "PARTY_GID")
    //      .join(encounter, patient("PATIENT_ID") === encounter("PATIENT_GID"))
    //      .groupBy(person("GIVEN_NAME"))
    //      .count()
    //      .show()


    val result_1 = patient
      .filter(patient("UPDATE_TS").isNotNull)
      .select(patient("UPDATE_TS"))
      .withColumn("SINCE_EPOCH", datediff(patient("UPDATE_TS"), from_unixtime(lit(0))))
    val result = result_1
      .withColumn("UPDATE_VIZ_EPOCH", result_1("UPDATE_TS") + result_1("SINCE_EPOCH"))


    result.show()
    result.printSchema()
  }
}
