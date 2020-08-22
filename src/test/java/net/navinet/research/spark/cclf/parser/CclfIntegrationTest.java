package net.navinet.research.spark.cclf.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.number.IsCloseTo.closeTo;

import com.google.gson.Gson;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileReader;

public class CclfIntegrationTest {
  private static JavaSparkContext sparkContext;
  private static SQLContext sqlContext;
  private static FixedWidthParserHelper parser;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    SparkConf sparkConf = new SparkConf()
        .setAppName("CCLF Smoke Test")
        .setMaster("local");
    sparkContext = new JavaSparkContext(sparkConf);
    sqlContext = new SQLContext(sparkContext);

    FormatLine[] schema = new Gson()
        .fromJson(new FileReader("src/test/resources/claims_header_file_schema.json"),
            MockFormatLine[].class);
    parser = new FixedWidthParserHelper(schema);
  }

  @AfterClass
  public static void haltSparkContext() {
    sparkContext.stop();
  }


  // Helper function to create rows from sample files
  private Row setRow(String path) {
    DataFrame dataFrame = sqlContext.read()
        .format("cclf")
        .option("path", path)
        .option("schema_path", "claims_header_file_schema.json")
        .load();
    return dataFrame.collect()[0];
  }


  @Test
  public void testReadingFromFilePutsCorrectContentInRow() throws Exception {
    Row row = this.setRow("src/test/resources/sample_claims_header_file.cclf");

    assertThat(row.getAs("CUR_CLM_UNIQ_ID"), hasToString("9999999999999"));
    assertThat(row.getAs("PRVDR_OSCAR_NUM"), hasToString("aaaaaa"));
    assertThat(row.getAs("CLM_TYPE_CD"), hasToString("88"));
    assertThat(row.getAs("CLM_FROM_DT"), hasToString("1995-10-21"));
    assertThat(row.<Double>getAs("CLM_PMT_AMT"), is(closeTo(200, 1)));
  }

  @Test
  public void testCanWriteRowIntoString() {
    Row fromFile = this.setRow("src/test/resources/sample_claims_header_file.cclf");

    String formatted = parser.format(fromFile);

    Row readBack = parser.parseLine(formatted);

    assertThat(fromFile.length(), is(readBack.length()));
    assertThat(formatted.length(), is(176));
  }

  @Test
  public void testCanWriteBackFormatAfterDroppingColumns() {
    DataFrame dataFrame = sqlContext.read()
        .format("cclf")
        .option("path", "src/test/resources/sample_claims_header_file.cclf")
        .option("schema_path", "claims_header_file_schema.json")
        .load();
    Row preDropRow = dataFrame.collect()[0];

    dataFrame = dataFrame.select("CUR_CLM_UNIQ_ID", "CLM_FROM_DT");

    Row row = dataFrame.collect()[0];

    String formatted = parser.format(row);
    Row readBack = parser.parseLine(formatted);

    assertThat(formatted.length(), is(176));
    assertThat(readBack.length(), is(equalTo(preDropRow.length())));
  }

  @Ignore
  @Test
  public void testThrowsExceptionOnTypeErrorsWhenReading() {
    DataFrame dataFrame = sqlContext.read()
      .format("cclf")
      .option("path", "src/test/resources/illegal_type_claims_header_file.cclf")
      .option("schema_path", "claims_header_file_schema.json")
      .load();
    expectedException.expect(SparkException.class);
    expectedException.expectCause(isA(NumberFormatException.class));
    dataFrame.show();
  }

  @Test
  public void testThrowsExceptionOnFormatErrorWhenReading() {
    DataFrame dataFrame = sqlContext.read()
      .format("cclf")
      .option("path", "src/test/resources/bad_format_claims_header_file.cclf")
      .option("schema_path", "claims_header_file_schema.json")
      .load();
    expectedException.expect(SparkException.class);
    expectedException.expectCause(isA(IllegalArgumentException.class));
    // Cause should be
    dataFrame.show();
  }

  @Test
  public void testThrowsIllegalArgumentExceptionOnSchemaFormatErrorWhenReading() {
    // Loading the schema is eager
    expectedException.expect(IllegalArgumentException.class);
    sqlContext.read()
      .format("cclf")
      .option("path", "src/test/resources/sample_claims_header_file.cclf")
      .option("schema_path", "bad_schema_file.json")
      .load();
  }

  @Test
  public void testThrowsIllegalArgumentExceptionOnSchemaFormatErrorWhenWriting() {
    DataFrame dataFrame = sqlContext.read()
      .format("cclf")
      .option("path", "src/test/resources/sample_claims_header_file.cclf")
      .option("schema_path", "claims_header_file_schema.json")
      .load();

    expectedException.expect(IllegalArgumentException.class);
    dataFrame.write()
      .format("cclf")
      .option("path", "src/test/resources/output_claims_header_file.cclf")
      .option("schema_path", "bad_schema_file.json")
      .save();
  }

  @Test
  public void testCanParseNumbersOnLinesContainingSpaces() {
    DataFrame dataFrame = sqlContext.read()
      .format("cclf")
      .option("path", "src/test/resources/sample_claims_header_file_spaces.cclf")
      .option("schema_path", "claims_header_file_schema.json")
      .load();

    assertThat(dataFrame, is(notNullValue()));

    Row[] rows = dataFrame.collect();
    assertThat(rows, is(arrayWithSize(1)));

    for (Row row : rows) {
      assertThat(row.size(), is(equalTo(29)));
    }
  }
}
