# This file is intended to generate large quantities of data to be used in testing the Spark job.
#
# In order to generate enough data, you will likely need to play around with the settings. With the
# settings that are currently in-place, it generates ~80 MB of data in ~15 minutes running from 1
# worker.
#
# You will also need to change the connection settings. You should create the table in the database
# in question before hand. You may not _need_ to do so, because the job will insert them. However,
# without the appropriate settings on the database, this code cannot guarantee that unique primary
# keys are going to be generated. So I recommend creating the tables, applying appropriate settings,
# then using this to perform the inserts.
#
# Usage:
# spark-submit \
#   --deploy-mode client \
#   --master spark://<master IP>:7077 \
#   --driver-class-path <Path to MySQL JDBC Driver> \
#   --conf spark.executor.extraClassPath=<Path to MySQL JDBC Driver> \
#   --jars <Path to MySQL JDBC Driver> \
#   <Path Here>/spark_generate_data.py






import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
import random
from datetime import *

sc = SparkContext("spark://192.168.33.10:7077", "Generate Fake Data")
sqlContext = SQLContext(sc)

# Control variables
# 
NUM_ITERATIONS = 10
NUM_PATIENTS = 99999999
BATCH_SIZE = 2
NUM_ENCOUNTERS = 60
PERCENT_HEARTRATE = 20
HEARTRATE_DURATION = 24 # 6 /min * 60 min/hr * 24 hr/day
HOST = "jdbc:mysql://127.0.0.1:3306/test_data?useServerPrepStmts=false&rewriteBatchedStatements=true"
USER = "root"
PASSWORD = "password"
DATABASE = "test_data"

def main():
    print("Starting job.")
    #TODO parallelize
    #sc.parallelize(range(1, BATCH_SIZE)).foreach(build_batch)
    for x in range(0, BATCH_SIZE):
        build_batch(x)
    print("Finished job successfully.")
    sc.stop()

def build_batch(i):
    demographics = []
    encounters = []
    heartrates = []
    print "Running batch: ", i
    for x in range(0, NUM_ITERATIONS):
        patient_id = random.randint(1, NUM_PATIENTS) # patient_mrn
        demographic = generate_demographic(patient_id)
        encounter = generate_encounter(patient_id)
        generate_heartrate_one(heartrates, patient_id, encounter)
        demographics.append(demographic)
        encounters.append(encounter)

    demographicsFrame = sqlContext.createDataFrame(demographics,
      ["patient_id", "first_name", "last_name", "dob", "gender", "race", "zip"])
    encountersFrame = sqlContext.createDataFrame(encounters,
      ["patient_id", "physician_id", "location_id", "visit_type", "encounter_date", "notes"])
    heartratesFrame = sqlContext.createDataFrame(heartrates, ["patient_id", "timestamp", "heartrate"])

    save_table(demographicsFrame, "demographics")
    save_table(encountersFrame, "encounters")
    save_table(heartratesFrame, "heartrate_ts")

    demographicsFrame.unpersist()
    encountersFrame.unpersist()
    heartratesFrame.unpersist()
  
def generate_demographic(patient_id):
    """
    Int -> Tuple<PatientID, FirstName, LastName, Date, Gender, Race, Zip>
    Generates max_num rows for the demographics table
    """
    ls = []
    first_names = ["Sam", "Leslie", "Regan", "Alex", "Sean"]
    last_names = ["Lung", "Chiu", "Rosenblum", "Kindy", "O'Riordan", "Allen"]
    genders = ["M", "F"]
    races = ["Hispanic", "Eskimo"]
    demographic = (
            patient_id,
            random.choice(first_names), # first name
            random.choice(last_names), # last name
            date(random.randint(1950, 1999), random.randint(1, 12), random.randint(1, 28)),
            random.choice(genders), # gender
            random.choice(races), # race
            random.randint(10000, 99999) # Zip
          )
    return demographic


def generate_encounter(patient_id):
    """
    Int, INT, INT, VARCHAR, DATE, VARCHAR -> Tuple<PatientID, PhysicianID, LocationID, VisitType, Date, Note>
    Generates max_num rows for the encounters table
    """
    visit_types = ["Office Visit", "Test", "ER"]
    notes = """
    Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus aliquet
    lectus vitae venenatis ultrices. Praesent facilisis luctus diam, eu molestie
    justo efficitur quis.
    """
    encounter = (
          patient_id,
          random.randint(1000000, 99999999), # physician_id
          random.randint(1000000, 99999999), # location_id
          random.choice(visit_types), # visit_types
          date(
            random.randint(2000, 2016),
            random.randint(1, 12),
            random.randint(1, 28)),
          notes
        )
    return encounter

def generate_heartrate_one(heartrates, patient_id, encounter):
    """
    Int, Datetime, Int -> List<Tuple<PatientID, EncounterID, DateTime, HeartRate>>
    Build a partial table of heartrates, for a single encounter
    """
    initial_ts = datetime.combine(encounter[4], time())
    for ts in range(0, HEARTRATE_DURATION):
        heartrates.append((
          patient_id,
          initial_ts + timedelta(seconds=(10 * ts)),
          random.randint(40, 100)
        ))
    return heartrates

def save_table(dataframe, tablename):
    (dataframe.write
      .jdbc(HOST, tablename, mode="append", properties={
        "user": USER, "password": PASSWORD, "driver":'com.mysql.jdbc.Driver'
      }))


if __name__ == '__main__':
    main()
