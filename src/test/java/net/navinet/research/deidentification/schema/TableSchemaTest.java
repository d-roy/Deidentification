package net.navinet.research.deidentification.schema;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import net.navinet.research.deidentification.deidentify.ColumnStrategy;
import net.navinet.research.deidentification.deidentify.ColumnStrategyBuilder;
import net.navinet.research.deidentification.deidentify.transforms.DataFrameTransformFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TableSchemaTest {
  private final String inboundConfig = "{\n"
    + "\"COS_PATIENT\": {\n"
    + "\"PARTY_GID\": {\n"
    + "\"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"gid\"}]\n"
    + "}, \n"
    + "\"PATIENT_ID\": {\n"
    + "\"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"patient_id\"}]\n"
    + "}}, \n"
    + "\"COS_PERSON\": {\n"
    + "\"FIRST_NAME\": {\n"
    + "\"strategy\": [{\"type\": \"RandomFirstName\"}]\n"
    + "}, \n"
    + "\"BIRTH_DATE\": {\n"
    + "\"strategy\": [{\"type\": \"DateEpoch\", \"date_epoch\": \"BIRTH_DATE\"}]\n"
    + "}\n"
    + "}}";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private JavaSparkContext sparkContext;
  private SQLContext sqlContext;
  private DataFrameTransformFactory factory;

  @Before
  public void setUp() throws Exception {
    SparkConf conf = new SparkConf()
        .setAppName("Test TransformFactory")
        .setMaster("local");
    this.sparkContext = new JavaSparkContext(conf);
    this.sqlContext = new SQLContext(this.sparkContext);

    this.factory = new DataFrameTransformFactory(sqlContext);
  }

  @After
  public void tearDown() throws Exception {
    this.sparkContext.stop();
  }

  @Test
  public void testConstructorThrowsExceptionOnNullColumnStrategyList() {
    expectedException.expect(NullPointerException.class);
    new TableSchema(null, this.factory);
  }

  @Test
  public void testConstructorThrowsExceptionOnNullFactory() {
    expectedException.expect(NullPointerException.class);
    new TableSchema(new ArrayList<ColumnStrategy>(), null);
  }

  @Test
  public void testDropsColumnsNotIncludedInConfiguration() {
    List<ColumnStrategy> strategies = new ColumnStrategyBuilder()
      .add("NAME", "RandomFirstName")
      .add("DATE_OF_BIRTH", "Year")
      .build();

    TableSchema tableSchema = new TableSchema(strategies, this.factory);

    DataFrame dataFrame = sqlContext
      .read()
      .json("src/test/resources/flow-sample-frame.json");

    DataFrame output = tableSchema.deidentify(dataFrame);

    String[] colsInOutput = output.columns();
    assertThat(colsInOutput, is(arrayWithSize(2)));
    assertThat(colsInOutput, is(arrayContainingInAnyOrder("NAME", "DATE_OF_BIRTH")));
  }

  @Test
  public void testTableSchemaPerformsMultipleDeidentifications() {
    List<ColumnStrategy> strategies = new ColumnStrategyBuilder()
      .add("NAME", "RandomFirstName")
      .add("DATE_OF_BIRTH", "Year")
      .add("PLACE_OF_BIRTH", "Zip3Digit")
      .build();
    TableSchema tableSchema = new TableSchema(strategies, this.factory);

    DataFrame dataFrame = sqlContext
      .read()
      .json("src/test/resources/flow-sample-frame.json");

    DataFrame output = tableSchema.deidentify(dataFrame);

    Row[] rows = output.collect();

    for (Row row : rows) {
      assertThat(row.size(), is(3));
      assertThat(row.<String>getAs("PLACE_OF_BIRTH"), is(anyOf(endsWith("00"), nullValue())));

      Timestamp value = row.getAs("DATE_OF_BIRTH");
      if (value == null) {
        assertThat(true, is(true));
      } else {
        assertThat(value.getDate(), is(1));
        assertThat(value.getMonth(), is(0));
      }
    }
  }
}