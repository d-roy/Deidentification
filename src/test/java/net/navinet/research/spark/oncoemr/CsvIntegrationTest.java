package net.navinet.research.spark.oncoemr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

public class CsvIntegrationTest {
  private static JavaSparkContext sparkContext;
  private static SQLContext sqlContext;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    SparkConf sparkConf = new SparkConf()
        .setAppName("CSV Smoke Test")
        .setMaster("local");
    sparkContext = new JavaSparkContext(sparkConf);
    sqlContext = new SQLContext(sparkContext);

  }

  @AfterClass
  public static void haltSparkContext() {
    sparkContext.stop();
  }


  // Helper function to create rows from sample files
  private Row setRow(String path) {
    DataFrame dataFrame = sqlContext.read()
        .format("com.databricks.spark.csv")
        .option("path", path).option("header", "true")
        .load();
    return dataFrame.collect()[0];
  }

  @Test
  public void testReadingFromFilePutsCorrectContentInRow() throws Exception {
    Row row = this.setRow("src/test/resources/sample_administration_data.csv");

    assertThat(row.getAs("ClientID"), hasToString("ME0001"));
    assertThat(row.getAs("AdministrationID"), hasToString("OCH11002725168211595_249"));
    assertThat(row.getAs("DiagnosisID"), hasToString("DH_I9M1105245029_004944"));
    assertThat(row.getAs("DoseAdministered"), hasToString("250"));
  }

}
