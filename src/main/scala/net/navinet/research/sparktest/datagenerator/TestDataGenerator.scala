package net.navinet.research.sparktest.datagenerator

import java.io.FileInputStream
import java.sql.{Date, Timestamp}
import java.util.{Calendar, GregorianCalendar, Properties}

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random.nextInt



/**
  * This is effectively the replacement for the Python script to generate the bulk fake data.
  * This has significantly better performance, which is actually not related to the different
  * languages. The problem was that the MySQL Connector did not automatically convert DataFrame
  * inserts into a database into a batch query, but rather was running them by inserting the
  * DataFrame a single line at a time, which was phenomenally bad for performance. Adding
  *
  * `?rewriteBatchedStatements=true`
  *
  * corrects the issue. At the time of writing, this generates ~ 50 MB of data in ~ 1 minute.
  *
  * As this job is on the JVM, rather than Python, this can be kicked off via the REST API using the
  * following call:
  *
  * `
  * {
  * "action": "CreateSubmissionRequest",
  * "appArgs": [],
  * "appResource": "file:/vagrant/DataImaging.jar",
  * "clientSparkVersion": "1.6.1",
  * "environmentVariables": {
  * "SPARK_ENV_LOADED": "1"
  * },
  * "mainClass": "net.navinet.research.sparktest.datagenerator.TestDataGenerator",
  * "sparkProperties": {
  * "spark.jars": "file:/vagrant/DataImaging.jar",
  * "spark.driver.supervise": "false",
  * "spark.app.name": "TestREST",
  * "spark.eventLog.enabled": "false",
  * "spark.submit.deployMode": "cluster",
  * "spark.master": "spark://192.168.50.5:6066",
  * "spark.driver.extraClassPath": "/vagrant/drivers/mysql-connector-java-6.0.2.jar",
  * "spark.executor.extraClassPath": "/vagrant/drivers/mysql-connector-java-6.0.2.jar"
  * }
  * }
  * `
  *
  * Alternatively, you can launch it via the CLI, which may be easier of you want to edit any of
  * the settings prior to running it. The command to launch via the CLI is
  *
  * `
  * spark-submit
  *   --master spark://SPARK_URL:7077 \
  *   --deploy-mode client \
  *   --jars PATH_TO_MYSQL_CONNECTOR \
  *   --conf spark.executor.extraClassPath=PATH_TO_MYSQL_CONNECTOR \
  *   --conf spark.driver.extraClassPath=PATH_TO_MYSQL_CONNECTOR \
  *   --class net.navinet.research.sparktest.datagenerator.TestDataGenerator \
  *   PATH_TO_JAR
  * `
  *
  */
object TestDataGenerator {
  def main(args: Array[String]) = {
    val properties = new Properties()
    properties.load(new FileInputStream(s"/tmp/data_generation.properties"))
    
    val host_address = properties.getProperty("address", "localhost")
    val host = s"jdbc:mysql://$host_address?useServerPrepStmts=false&rewriteBatchedStatements=true"
    val num_patients = Integer.parseInt(properties.getProperty("num_patients", "10"))
    val encountersPerPatient = Integer.parseInt(properties.getProperty("encounters_per_patient", "2"))
    val heartrateDuration = Integer.parseInt(properties.getProperty("heartrate_duration", "360"))

    val conf = new SparkConf()
      .setAppName("Generate Fake Data")

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", properties.getProperty("user"))
    connectionProperties.setProperty("password", properties.getProperty("password"))

    val patientDF = sparkContext.parallelize(1 to num_patients).map(i => generatePatient(i)).toDF()

    patientDF.write
        .mode(SaveMode.Overwrite)
        .jdbc(host, "demographics", connectionProperties)

    patientDF.unpersist(false)

    val num_encounters = num_patients * encountersPerPatient
    val encounterDF = sparkContext
      .parallelize(1 to num_encounters)
      .map(i => generateEncounter(i)).toDF()

    encounterDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(host, "encounters", connectionProperties)
    encounterDF.unpersist(false)

    val heartRatesDF = sparkContext
      .parallelize(1 to num_patients)
      .map(i => generateHeartRates(i, heartrateDuration))
      .flatMap(i => i)
      .toDF()

    heartRatesDF.write
        .mode(SaveMode.Overwrite)
        .jdbc(host, "heartrate_ts", connectionProperties)
    heartRatesDF.unpersist(false)

    sparkContext.stop()
  }

  def generatePatient(patientNum: Int): Patient = {
    val firstNames = List("Sam", "Leslie", "Regan", "Alex", "Sasha")
    val lastNames = List("Lung", "Chiu", "Rosenblum", "Kindy", "O'Riordan", "Allen")
    val genders = List("F", "M")
    val races = List("Hispanic", "Eskimo")

    val firstName = firstNames(nextInt(firstNames.size))
    val lastName = lastNames(nextInt(lastNames.size))
    val dobMillis = new GregorianCalendar(1950 + nextInt(49), nextInt(11), 1 + nextInt(27))
      .getTimeInMillis
    val dob = new Date(dobMillis)
    val gender = genders(nextInt(genders.size))
    val race = races(nextInt(races.size))



    Patient(
      patient_id = patientNum,
      patient_mrn = (1000000 + nextInt(8999999)).toString,
      firstName,
      lastName,
      dob,
      gender,
      race,
      zip = (10000 + nextInt(89999)).toString
    )
  }

  def generateEncounter(patient_id: Int): Encounter = {
    val visitTypes = List("Office Visit", "Test", "ER")
    val notes =
      """
        | Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus aliquet
        | lectus vitae venenatis ultrices. Praesent facilisis luctus diam, eu molestie
        | justo efficitur quis.
      """.stripMargin


    val visitType = visitTypes(nextInt(visitTypes.size))
    val encounterTS = new GregorianCalendar(2000 + nextInt(16), nextInt(11), 1 + nextInt(27))
      .getTimeInMillis
    val encounterDate = new Date(encounterTS)

    Encounter(
      patient_id = patient_id,
      physician_id = 1000000 + nextInt(8999999),
      location_id = 1000000 + nextInt(8999999),
      visitType,
      encounterDate,
      notes
    )

  }

 def generateHeartRates(patient_id: Int, heartrate_duration: Int) : Seq[Heartrate] = {
    val encounterTS = new GregorianCalendar(2000 + nextInt(16), nextInt(11), 1 + nextInt(27))
        .getTimeInMillis
    val encounterDate = new Date(encounterTS)
    val initialTimestamp = encounterDate.getTime

   (0 to heartrate_duration).map(heartrateNum => {

      val timestamp = new Timestamp(initialTimestamp + 60 * heartrateNum)

      Heartrate(
        patient_id = patient_id,
        timestamp = timestamp,
        heartrate = 40 + nextInt(60)
      )
    })
  }
}
