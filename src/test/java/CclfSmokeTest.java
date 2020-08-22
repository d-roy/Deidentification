import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.datediff;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CclfSmokeTest {
  private static JavaSparkContext sparkContext;
  private static SQLContext sqlContext;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() {
    SparkConf sparkConf = new SparkConf()
        .setAppName("CCLF Smoke Test")
        .setMaster("local");
    sparkContext = new JavaSparkContext(sparkConf);
    sqlContext = new SQLContext(sparkContext);
  }

  @AfterClass
  public static void haltSparkContext() {
    if (sparkContext != null) {
      sparkContext.stop();
    }

    try {
      Files.deleteIfExists(Paths.get("src/test/resources/generated_claims_reader_file.cclf"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testThrowsExceptionIfPathNotSpecified() {
    expectedException.expect(IllegalStateException.class);
    sqlContext.read()
        .format("net.navinet.research.spark.cclf.parser")
        .option("schema_path", "claims_header_file_schema.json")
        .load();
  }

  @Test
  public void testThrowsExceptionIfSchemaPathNotSpecified() {
    expectedException.expect(IllegalStateException.class);
    sqlContext.read()
        .format("net.navinet.research.spark.cclf.parser")
        .option("path", "src/test/resources/sample_claims_header_file.cclf")
        .load();
  }

  @Test
  public void testCanReadFromFile() {
    DataFrame dataFrame = sqlContext.read()
        .format("net.navinet.research.spark.cclf.parser")
        .option("path", "src/test/resources/sample_claims_header_file.cclf")
        .option("schema_path", "claims_header_file_schema.json")
        .load();

    assertThat(dataFrame, is(notNullValue()));

    List<Row> rows = dataFrame.collectAsList();
    assertThat(rows, hasSize(1));

    for (Row r : rows) {
      assertThat(r.length(), is(29));
    }
  }

  @Test
  public void testCanUseShortNameInsteadOfPackage() {
    DataFrame dataFrame = sqlContext.read()
        // Use short name fixed_width instead of net.navinet.research.spark.fixedwidth
        .format("cclf")
        .option("path", "src/test/resources/sample_claims_header_file.cclf")
        .option("schema_path", "claims_header_file_schema.json")
        .load();

    assertThat(dataFrame, is(notNullValue()));
    assertThat(dataFrame.collectAsList(), hasSize(1));
  }

  @Test
  public void testDataTypesAreAppliedCorrectly() {
    DataFrame dataFrame = sqlContext.read()
        .format("cclf")
        .option("path", "src/test/resources/sample_claims_header_file.cclf")
        .option("schema_path", "claims_header_file_schema.json")
        .load();

    DataFrame result = dataFrame
        .select(
            dataFrame.col("CUR_CLM_UNIQ_ID"),
            datediff(
                date_add(dataFrame.col("CLM_FROM_DT"), 5),
                dataFrame.col("CLM_FROM_DT")
            ).as("CLM_AGE")
        );

    List<Row> rows = result.collectAsList();

    for (Row row : rows) {
      assertThat(row.getInt(1), is(5));
    }
  }


  @Ignore
  @Test
  public void testWriteOutputCCLF() {
    DataFrame dataFrame = sqlContext.read()
            .format("cclf")
            .option("path", "src/test/resources/sample_claims_header_file.cclf")
            .option("schema_path", "claims_header_file_schema.json")
            .load();

    dataFrame.rdd().saveAsTextFile("newfile.txt");
    //dataFrame.coalesce(1).saveAsTextFile("newfile");

//    dataFrame.write()
//            .mode(SaveMode.Overwrite)
//            .format("cclf")
//            .option("schema_path", "claims_header_file_schema.json")
//            .option("path", "src/test/resources/myfile_header.cclf")
//            .save("myfile_header.cclf");

  }

//  @Test
//  public void testThrowsNullPointerExceptionIfCannotFindCCLFFile() {
//    DataFrame dataFrame = sqlContext.read()
//      .format("cclf")
//      .option("path", "NotARealFile.cclf")
//      .option("schema_path", "claims_header_file_schema.json")
//      .load();
//
//    expectedException.expect(NullPointerException.class);
//    dataFrame.count();
//  }
}
