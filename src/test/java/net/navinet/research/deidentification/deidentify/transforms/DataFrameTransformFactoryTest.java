package net.navinet.research.deidentification.deidentify.transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import net.navinet.research.deidentification.deidentify.MockColumnStrategy;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataFrameTransformFactoryTest {
  private static JavaSparkContext sparkContext;
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();
  private DataFrameTransformFactory factory;
  private DataFrame sampleFrame;
  private DataFrame flowSampleFrame;
  private DataFrame flowSampleFrame2;
  private DataFrame flowSampleFrame3;
  private DataFrame flowSampleFrame4;
  private DataFrame sampleFrameSpaces;

  @BeforeClass
  public static void setUp() {
    SparkConf conf = new SparkConf()
        .setAppName("Test TransformFactory")
        .setMaster("local");
    sparkContext = new JavaSparkContext(conf);
  }

  @AfterClass
  public static void tearDown() {
    sparkContext.stop();
  }

  @Before
  public void createBaseDataFrame() {
    SQLContext sqlContext = new SQLContext(sparkContext);
    this.sampleFrame = sqlContext.read().json("src/test/resources/sample-frame.json");
    this.flowSampleFrame = sqlContext.read().json("src/test/resources/flow-sample-frame.json");
    this.flowSampleFrame2 = sqlContext.read().json("src/test/resources/flow-sample-frame2.json");
    this.flowSampleFrame3 = sqlContext.read().json("src/test/resources/flow-sample-frame3.json");
    this.flowSampleFrame4 = sqlContext.read().json("src/test/resources/flow-sample-frame4.json");
    this.sampleFrameSpaces = sqlContext.read()
      .json("src/test/resources/sample-frame-trim-spaces.json");
    this.factory = new DataFrameTransformFactory(sqlContext);
  }

  @Test
  public void testConstructorThrowsExceptionOnNullArgument() {
    expectedException.expect(NullPointerException.class);
    new DataFrameTransformFactory(null);
  }


  @Test
  public void checkIdentityTransform() throws IOException {
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("NAME", "Identity"));

    DataFrame result = transform.transform(this.sampleFrame);

    List<String> actualRows = new ArrayList<>();
    for (Row row : result.collect()) {
      actualRows.add(row.toString());
    }

    List<String> expectedRows = new ArrayList<>();
    for (Row row : sampleFrame.collect()) {
      expectedRows.add(row.toString());
    }

    assertThat(actualRows, is(equalTo(expectedRows)));
  }

  @Test
  public void checkDropTransform() throws IOException{
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("NAME", "Drop"));

    DataFrame result = transform.transform(this.sampleFrame);
    assertThat(result.columns(), is(new String[]{"DATE_OF_BIRTH", "DATE_OF_SERVICE", "EMAIL"}));
  }

  @Test
  public void checkAgeTransform() throws IOException{
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("AGE", "Age"));

    DataFrame result = transform.transform(this.flowSampleFrame);
    for (Row row : result.collect()) {
      assertThat(row.<Long>getAs("AGE"),
          is(anyOf(lessThanOrEqualTo(90L), nullValue())));
    }
  }

  @Test
  public void checkDateScrubTransform() throws Exception {
    DateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
    DataFrameTransform transform =
        this.factory.createTransform(
            new MockColumnStrategy("DATE_OF_SERVICE", "Year")
                .dateEpoch("DATE_OF_BIRTH"));

    List<Row> rowsBefore = this.sampleFrame.collectAsList();

    DataFrame result = transform.transform(this.sampleFrame);
    List<Row> rows = result.collectAsList();

    for (int i = 0; i < rows.size(); i++) {
      String beforeElement = rowsBefore.get(i).getAs("DATE_OF_SERVICE");
      Timestamp afterElement = rows.get(i).getAs("DATE_OF_SERVICE");

      if (beforeElement == null) {
        assertThat(afterElement, is(nullValue()));
      } else {
        Calendar before = Calendar.getInstance();
        before.setTime(parser.parse(beforeElement));

        Calendar after = Calendar.getInstance();
        after.setTime(afterElement);

        // Actual assertion: YEAR is same, DAY_OF_YEAR changed to 1
        assertThat(after.get(Calendar.YEAR), is(before.get(Calendar.YEAR)));
        assertThat(after.get(Calendar.DAY_OF_YEAR), is(1));
      }
    }
  }

  @Test
  public void checkDateEpochTransform() throws IOException {
    DataFrameTransform transform =
        this.factory.createTransform(
            new MockColumnStrategy("DATE_OF_SERVICE", "DateEpoch")
                .dateEpoch("DATE_OF_BIRTH"));

    DataFrame result = transform.transform(this.sampleFrame);

    List<Row> rowsBefore = this.sampleFrame.collectAsList();
    List<Row> rows = result.collectAsList();

    for (int i = 0; i < rows.size(); i++) {
      String epochStartString = rowsBefore.get(i).getAs("DATE_OF_BIRTH");
      String preTransformString = rowsBefore.get(i).getAs("DATE_OF_SERVICE");
      Integer afterTransform = rows.get(i).getAs("DATE_OF_SERVICE");

      if (epochStartString == null || preTransformString == null) {
        assertThat(afterTransform, is(nullValue()));
      } else {
        LocalDateTime epochStart = LocalDateTime.parse(epochStartString);
        LocalDateTime preTransform = LocalDateTime.parse(preTransformString);

        assertThat(afterTransform,
          is(equalTo(Days.daysBetween(epochStart, preTransform).getDays() + 1)));
      }
    }
  }

@Test
public void checkDateDayTransform() {
  final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  DataFrameTransform transform =
      this.factory.createTransform(
          new MockColumnStrategy("DATE_OF_SERVICE", "DateDay"));

  DataFrame result = transform.transform(this.sampleFrame);
  List<Row> rowsBefore = this.sampleFrame.collectAsList();
  List<Row> rowsAfter = result.collectAsList();

  for (int i = 0; i < rowsAfter.size(); i++) {
      if(rowsBefore.get(i).getAs("DATE_OF_SERVICE") != null) {
        String dateBefore = rowsBefore.get(i).getAs("DATE_OF_SERVICE");
        String dateAfter = rowsAfter.get(i).getAs("DATE_OF_SERVICE").toString();

        int expected = 0;
        assertThat(dateBefore.compareTo(dateAfter), is(not(equalTo(expected))));
      } else {
        assertThat(rowsAfter.get(i).getAs("DATE_OF_SERVICE"), nullValue());
      }
  }

}

  @Test
  public void checkOncoEMRDateShiftTransform() {
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    DataFrameTransform transform =
        this.factory.createTransform(
            new MockColumnStrategy("DATE_OF_SERVICE", "DateShift"));

    DataFrame result = transform.transform(this.flowSampleFrame4);
    List<Row> rowsBefore = this.flowSampleFrame4.collectAsList();
    List<Row> rowsAfter = result.collectAsList();

    for (int i = 0; i < rowsAfter.size(); i++) {
      if(rowsBefore.get(i).getAs("DATE_OF_SERVICE") != null) {
        String dateBefore = rowsBefore.get(i).getAs("DATE_OF_SERVICE");
        String dateAfter = rowsAfter.get(i).getAs("DATE_OF_SERVICE").toString();

        int expected = 0;
        assertThat(dateBefore.compareTo(dateAfter), is(not(equalTo(expected))));
      } else {
        assertThat(rowsAfter.get(i).getAs("DATE_OF_SERVICE"), nullValue());
      }
    }

  }

  @Test
  public void checkOncoEMRDateTimeShiftTransform() {
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    DataFrameTransform transform =
        this.factory.createTransform(
            new MockColumnStrategy("DATE_OF_BIRTH", "DateTimeShift"));

    DataFrame result = transform.transform(this.flowSampleFrame4);
    List<Row> rowsBefore = this.flowSampleFrame4.collectAsList();
    List<Row> rowsAfter = result.collectAsList();

    for (int i = 0; i < rowsAfter.size(); i++) {
      if(rowsBefore.get(i).getAs("DATE_OF_BIRTH") != null) {
        String dateTimeBefore = rowsBefore.get(i).getAs("DATE_OF_BIRTH");
        String dateTimeAfter = rowsAfter.get(i).getAs("DATE_OF_BIRTH").toString();

        int expected = 0;
        assertThat(dateTimeBefore.compareTo(dateTimeAfter), is(not(equalTo(expected))));
      } else {
        assertThat(rowsAfter.get(i).getAs("DATE_OF_BIRTH"), nullValue());
      }
    }

  }


  @Test
  public void checkOncoEMRNameTransform() {
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    DataFrameTransform transform =
        this.factory.createTransform(
            new MockColumnStrategy("NAME", "oncoemrFirstName"));

    DataFrame result = transform.transform(this.flowSampleFrame4);
    List<Row> rowsBefore = this.flowSampleFrame4.collectAsList();
    List<Row> rowsAfter = result.collectAsList();

    for (int i = 0; i < rowsAfter.size(); i++) {
      if(rowsBefore.get(i).getAs("NAME") != null) {
        String nameBefore = rowsBefore.get(i).getAs("NAME");
        String nameAfter = rowsAfter.get(i).getAs("NAME").toString();

        int expected = 0;
        assertThat(nameBefore.compareTo(nameAfter), is(not(equalTo(expected))));
      } else {
        assertThat(rowsAfter.get(i).getAs("NAME"), nullValue());
      }
    }

  }


  @Test
  public void checkEpochTransform() throws IOException {
    DataFrameTransform transform =
        this.factory.createTransform(new MockColumnStrategy("DATE_OF_BIRTH", "EpochStart"));

    DataFrame result = transform.transform(this.sampleFrame);

    List<Row> rows = result.collectAsList();

    assertThat(rows.get(0).<Date>getAs("EPOCH_START").getYear(),
        is(both(greaterThanOrEqualTo(8)).and(lessThanOrEqualTo(12))));
    assertThat(rows.get(1).<Date>getAs("EPOCH_START").getYear(),
        is(both(greaterThanOrEqualTo(110)).and(lessThanOrEqualTo(114))));
    assertThat(rows.get(2).<Date>getAs("EPOCH_START").getYear(),
        is(both(greaterThanOrEqualTo(83)).and(lessThanOrEqualTo(87))));

  }

  @Test
  public void checkHashTransform() throws IOException {
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("NAME", "Hash").saltKey("NAME"));

    DataFrame result = transform.transform(this.sampleFrame);

    // Null / empty rows are not hashed, so will occur in both tables.
    // This is expected behavior.
    result = result
        .filter(result.col("NAME").isNull())
        .filter(result.col("NAME").equalTo(""));

    List<Row> intersection = result.intersect(this.sampleFrame).collectAsList();
    assertThat(intersection, hasSize(0));

    assertThat(intersection, is(empty()));
  }

  @Test
  public void checkPBEKeyHashTransform() throws IOException {
    DataFrameTransform transform = this.factory
      .createTransform(new MockColumnStrategy("NAME", "PBEKeyHash").saltKey("NAME"));

    DataFrame result = transform.transform(this.sampleFrame);

    // Null / empty rows are not hashed, so will occur in both tables.
    // This is expected behavior.
    result = result
      .filter(result.col("NAME").isNull())
      .filter(result.col("NAME").equalTo(""));

    List<Row> intersection = result.intersect(this.sampleFrame).collectAsList();
    assertThat(intersection, hasSize(0));

    assertThat(intersection, is(empty()));
  }

  @Test
  public void checkHashTransformIgnoresTrailingSpacesWhenPerformingHash() throws IOException {
    DataFrameTransform transform = this.factory
      .createTransform(new MockColumnStrategy("NAME", "Hash").saltKey("NAME"));
    
    Row[] resultRows = transform.transform(sampleFrameSpaces).collect();

    assertThat(resultRows[0].<String>getAs("NAME"),
      is(equalTo(resultRows[1].<String>getAs("NAME"))));
  }

  @Test
  public void checkDuplicateTransform() throws IOException {
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("NAME", "Duplicate"));


    DataFrame result = transform.transform(this.sampleFrame);
    List<String> columns = Arrays.asList(result.columns());
    assertThat(columns,
        containsInAnyOrder("NAME", "NAME_duplicate", "EMAIL", "DATE_OF_BIRTH", "DATE_OF_SERVICE"));
  }

  @Test @SuppressWarnings("unchecked")
  public void checkZip3Transform() throws IOException {
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "Zip3Digit"));

    DataFrame result = transform.transform(this.flowSampleFrame);
    for (Row row : result.collect()) {
      String transformedPlaceOfBirth = row.getAs("PLACE_OF_BIRTH");
      if (transformedPlaceOfBirth != null) {
        assertThat(transformedPlaceOfBirth.length(), is(5));
        assertThat(transformedPlaceOfBirth.substring(3), is("00"));
        assertThat(transformedPlaceOfBirth, not(anyOf(
          containsString("036"),
          containsString("692"),
          containsString("878"),
          containsString("059"),
          containsString("790"),
          containsString("879"),
          containsString("063"),
          containsString("821"),
          containsString("884"),
          containsString("102"),
          containsString("823"),
          containsString("890"),
          containsString("203"),
          containsString("830"),
          containsString("893"),
          containsString("556"),
          containsString("831")
          )));
      }
    }
  }

  @Test
  public void checkFirstNameTransform() throws IOException {
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "RandomFirstName"));

    DataFrame result = transform.transform(this.flowSampleFrame);
    for (Row row : result.collect()) {
      assertThat(row.<String>getAs("PLACE_OF_BIRTH"), isIn(FirstNameTransform.firstNames));
    }
  }

  @Test
  public void checkEthnicityTransform() throws IOException{
      DataFrameTransform transform = this.factory
              .createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "Ethnicity"));
      DataFrame result = transform.transform(this.flowSampleFrame2);
      for (Row row : result.collect()){
        String transformedEthnicity = row.getAs("PLACE_OF_BIRTH");
        if(transformedEthnicity != null){
          assertThat(transformedEthnicity, is(equalToIgnoringCase("Black or African American")));
        }

      }

  }


  @Test
  public void checkCityTransform() throws IOException{
    DataFrameTransform transform = this.factory
            .createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "City"));
    DataFrame result = transform.transform(this.flowSampleFrame3);
    for (Row row : result.collect()){
      String transformedCity = row.getAs("PLACE_OF_BIRTH");
      if (transformedCity != null){
      assertThat(transformedCity.length(), is(greaterThan(0)));
  }

  }}





  @Test
  public void checkLastNameTransform() throws IOException {
    DataFrameTransform transform = this.factory
        .createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "RandomLastName"));

    DataFrame result = transform.transform(this.flowSampleFrame);

    String[] lastNames = {
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Miller",
        "Davis",
        "Garcia",
        "Rodriguez",
        "Wilson",
        "Martinez",
        "Anderson",
        "Taylor",
        "Thomas",
        "Hernandez",
        "Moore"
      };

    for (Row row : result.collect()) {
      assertThat(row.<String>getAs("PLACE_OF_BIRTH"), isIn(lastNames));
    }
  }

  @Test
  public void checkFixedFirstNameTransform() throws IOException {
    DataFrameTransform transform = this.factory
      .createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "FixedFirstName"));

    DataFrame result = transform.transform(this.flowSampleFrame);

    for (Row row : result.collect()) {
      assertThat(row.<String>getAs("PLACE_OF_BIRTH"), is("FirstName"));
    }
  }

  @Test
  public void checkFixedLastNameTransform() throws IOException {
    DataFrameTransform transform = this.factory
      .createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "FixedLastName"));

    DataFrame result = transform.transform(this.flowSampleFrame);

    for (Row row : result.collect()) {
      assertThat(row.<String>getAs("PLACE_OF_BIRTH"), is("LastName"));
    }
  }

  @Test
  public void checkMaxYearTransform() throws IOException{
    DataFrameTransform transform = this.factory
      .createTransform(new MockColumnStrategy("YEAR_OF_BIRTH", "MaxYear"));

    DataFrame result = transform.transform(this.flowSampleFrame);

    long currentYear = new GregorianCalendar().get(Calendar.YEAR);

    for (Row row : result.collect()) {
      assertThat(row.<Long>getAs("YEAR_OF_BIRTH"),
        is(anyOf(greaterThanOrEqualTo(currentYear - 90), nullValue())));
    }
  }

  @Test
  public void checkBadTransformType() throws IOException{
    expectedException.expect(IllegalArgumentException.class);
    this.factory.createTransform(new MockColumnStrategy("PLACE_OF_BIRTH", "NotAStrategy"));
  }
}