package net.navinet.research.deidentification.connection;

import static org.hamcrest.Matchers.hasToString;
import static org.junit.Assert.assertThat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.StringReader;

public class ConnectionFactoryTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private JavaSparkContext sparkContext;
  private SQLContext sqlContext;

  @Before
  public void setUp() throws Exception {
    SparkConf conf = new SparkConf()
        .setAppName("Test TransformFactory")
        .setMaster("local");
    this.sparkContext = new JavaSparkContext(conf);
    this.sqlContext = new SQLContext(this.sparkContext);
  }

  @After
  public void tearDown() throws Exception {
    this.sparkContext.stop();
  }

  @Test
  public void testConstructorThrowsExceptionOnNullArgument() {
    expectedException.expect(NullPointerException.class);
    new ConnectionFactory(null);
  }

  @Test
  public void testFactoryMethodThrowsExceptionOnNullArgument() {
    expectedException.expect(NullPointerException.class);
    new ConnectionFactory(this.sqlContext).fromConfiguration(null);
  }

  @Test
  public void testFactoryMethodSuccessfullyCreatesJdbcConnection() {
    ConnectionFactory factory = new ConnectionFactory(this.sqlContext);
    Connection connection = factory.fromConfiguration(new StringReader("{\n"
        + "    \"driver\": \"com.mysql.jdbc.Driver\",\n"
        + "    \"connection_url\": \"jdbc:mysql://localhost:3306/COS_DB\",\n"
        + "    \"username\": \"foo\",\n"
        + "    \"password\": \"bar\",\n"
        + "    \"format\": \"jdbc\"\n"
        + "}"));

    assertThat(connection, hasToString("[JDBC - jdbc:mysql://localhost:3306/COS_DB]"));
  }

  @Test
  public void testFactoryMethodThrowsExceptionOnBadFormatType() {
    ConnectionFactory factory = new ConnectionFactory(this.sqlContext);

    expectedException.expect(IllegalArgumentException.class);
    Connection connection = factory.fromConfiguration(new StringReader("{\n"
        + "    \"driver\": \"com.mysql.jdbc.Driver\",\n"
        + "    \"connection_url\": \"output.json\",\n"
        + "    \"format\": \"fake\"\n"
        + "}"));
  }
}