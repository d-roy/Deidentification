package net.navinet.research.spark.cclf.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.gson.Gson;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

public class FixedWidthParserHelperTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private FixedWidthParserHelper parser;

  @Before
  public void setUp() throws IOException {
    FormatLine[] schema = new Gson()
        .fromJson(new FileReader("src/test/resources/claims_header_file_schema.json"),
            MockFormatLine[].class);
    this.parser = new FixedWidthParserHelper(schema);
  }

  @Test
  public void testCreateParserWithNullSchemaThrowsWException() {
    expectedException.expect(NullPointerException.class);
    new FixedWidthParserHelper(null);
  }

  @Test
  public void testSchemaCreatesWithAllFields() throws Exception {
    StructType schema = this.parser.schema();


    assertThat(schema.length(), is(29));
    assertThat(schema.fieldNames(), is(arrayContaining(
        "CUR_CLM_UNIQ_ID",
        "PRVDR_OSCAR_NUM",
        "BENE_HIC_NUM",
        "CLM_TYPE_CD",
        "CLM_FROM_DT",
        "CLM_THRU_DT",
        "CLM_BILL_FAC_TYPE_CD",
        "CLM_BILL_CLSFCTN_CD",
        "PRNCPL_DGNS_CD",
        "ADMTG_DGNS_CD",
        "CLM_MDCR_NPMT_RSN_CD",
        "CLM_PMT_AMT",
        "CLM_NCH_PRMRY_PYR_CD",
        "PRVDR_FAC_FIPS_ST_CD",
        "BENE_PTNT_STUS_CD",
        "DGNS_DRG_CD",
        "CLM_OP_SRVC_TYPE_CD",
        "FAC_PRVDR_NPI_NUM",
        "OPRTG_PRVDR_NPI_NUM",
        "ATNDG_PRVDR_NPI_NUM",
        "OTHR_PRVDR_NPI_NUM",
        "CLM_ADJSMT_TYPE_CD",
        "CLM_EFCTV_DT",
        "CLM_IDR_LD_DT",
        "BENE_EQTBL_BIC_HICN_NUM",
        "CLM_ADMSN_TYPE_CD",
        "CLM_ADMSN_SRC_CD",
        "CLM_BILL_FREQ_CD",
        "CLM_QUERY_CD"
    )));
    // Checking all the different types, rather than all the different fields
    assertThat(schema.apply("PRVDR_OSCAR_NUM").dataType(),
        is(instanceOf(DataTypes.StringType.getClass())));
    assertThat(schema.apply("CLM_EFCTV_DT").dataType(),
        is(instanceOf(DataTypes.DateType.getClass())));
    assertThat(schema.apply("CLM_PMT_AMT").dataType(),
        is(instanceOf(DataTypes.DoubleType.getClass())));
    assertThat(schema.apply("CLM_TYPE_CD").dataType(),
        is(instanceOf(DataTypes.LongType.getClass())));
  }

  @Test
  public void testParseLineThrowsExceptionOnNullArgument() {
    expectedException.expect(NullPointerException.class);
    this.parser.parseLine(null);
  }

  @Test
  public void testParseLineCreatesRowsWithCorrectContents() throws Exception {
    InputStream inputStream = this
        .getClass()
        .getClassLoader()
        .getResourceAsStream("sample_claims_header_file.cclf");
    String sampleLine = new Scanner(inputStream).nextLine();
    inputStream.close();


    Row row = this.parser.parseLine(sampleLine);
    assertThat(row.length(), is(29));
    assertThat(row.anyNull(), is(false));
  }

  @Test
  public void testParseLineCanCreateRowFromLineWithSpaces() {
    String input =
      "9999999999999     0          0001995-10-211969-12-3100      0      0"
        + " 000000000000000.000 0 0   00         0         0         0         0"
        + " 01969-12-311969-12-31          0 0 000";

    Row output = this.parser.parseLine(input);

    assertThat(output.length(), is(equalTo(29)));
  }

  @Test
  public void testFormatWithNullArgumentThrowsException() {
    expectedException.expect(NullPointerException.class);
    this.parser.format(null);
  }
}