package net.navinet.research.deidentification.schema;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import com.google.gson.JsonSyntaxException;
import net.navinet.research.deidentification.deidentify.ColumnStrategy;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.StringReader;
import java.util.List;

public class SchemaAccessorTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  

  private final String multipleConfig = "{\n" +
    "  \"COS_PATIENT\" : {\n" +
    "    \"_meta\": {\n" +
    "      \"partitioningColumn\": \"PARTY_GID\"\n" +
    "    }, \n" +
    "    \"PARTY_GID\": {\n" +
    "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"gid\"}]\n" +
    "    }, \n" +
    "    \"PATIENT_ID\": {\n" +
    "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"patient_id\"}]\n" +
    "    }\n" +
    "  }, \n" +
    "  \"COS_PERSON\": {\n" +
    "    \"FIRST_NAME\": {\n" +
    "      \"strategy\": [{\"type\": \"RandomFirstName\"}]\n" +
    "    }, \n" +
    "    \"BIRTH_DATE\": {\n" +
    "      \"strategy\": [{\"type\": \"DateEpoch\", \"date_epoch\": \"BIRTH_DATE\"}]\n" +
    "    }\n" +
    "  }\n" +
    "}";

  private final String singleConfig = "{" +
    "\"CUR_CLM_UNIQ_ID\":{\"strategy\":[{\"type\":\"Hash\",\"salt_key\":\"clm_key\"}]},\n" +
    "\"BENE_HIC_NUM\":{\"strategy\":[{\"type\":\"Zip3Digit\",\"salt_key\":\"hic_key\"}]},\n" +
    "\"FAC_PRVDR_NPI_NUM\":{\"strategy\":[{\"type\":\"Year\",\"salt_key\":\"npi_key\"}]}\n" +
    "}";

  private final SchemaAccessor schemaAccessor = new SchemaAccessor();

  @Test @SuppressWarnings("unchecked") // Ignore unchecked generics array creation
  public void testCanReadContainingOnlyColumnStrategies() {
    List<ColumnStrategy> strategies = schemaAccessor
      .readSingleConfiguration(new StringReader(singleConfig));

    assertThat(strategies, hasSize(3));

    // Explicitly written out to get the type checker to behave itself
    Matcher<ColumnStrategy> currentClaimId =
      hasToString("ColumnStrategySchemaImpl{name='CUR_CLM_UNIQ_ID', type='Hash'}");
    Matcher<ColumnStrategy> beneficiaryHicNum =
      hasToString("ColumnStrategySchemaImpl{name='BENE_HIC_NUM', type='Zip3Digit'}");
    Matcher<ColumnStrategy> providerNpi =
      hasToString("ColumnStrategySchemaImpl{name='FAC_PRVDR_NPI_NUM', type='Year'}");

    assertThat(strategies, hasItems(currentClaimId, beneficiaryHicNum, providerNpi));
  }

  @Test
  public void testReadSingleConfigurationThrowsExnOnNullArgument() {
    expectedException.expect(NullPointerException.class);
    schemaAccessor.readSingleConfiguration(null);
  }

  @Test
  public void testReadMultipleConfigurationThrowsExnOnNullArguments() {
    expectedException.expect(NullPointerException.class);
    schemaAccessor.readMultipleConfiguration(null);
  }

  @Test @SuppressWarnings("unchecked") // Ignore unchecked generics array creation
  public void testCanReadMultipleConfigurationCanReadSchemaFileWithMultipleTables() {
    List<FrameConfiguration> configurations = schemaAccessor
      .readMultipleConfiguration(new StringReader(multipleConfig));

    Matcher<FrameConfiguration> cosPatient =
      hasToString("FrameConfiguration{name='COS_PATIENT', partitioningColumn='PARTY_GID'}");
    Matcher<FrameConfiguration> cosPerson =
      hasToString("FrameConfiguration{name='COS_PERSON', partitioningColumn='null'}");

    assertThat(configurations, hasSize(2));
    assertThat(configurations, hasItems(cosPerson, cosPatient));

    for (FrameConfiguration config : configurations) {

      assertThat(config.columnStrategies(), hasSize(2));
      for (ColumnStrategy strategy : config.columnStrategies()) {
        assertThat(strategy.getColumnName(), is(notNullValue()));
        assertThat(strategy.getType(), is(notNullValue()));
      }

      assertThat(config.name(), is(notNullValue()));
    }
  }

  @Test
  public void readMultipleConfigurationsDiscardsUnknownKeysInMetaBlock() {
    String configExtraMetaKey = "{\n" +
      "  \"COS_PATIENT\" : {\n" +
      "    \"_meta\": {\n" +
      "      \"partitioningColumn\": \"PARTY_GID\",\n" +
      "      \"bogusKey\": \"bogusValue\"\n" +
      "    }, \n" +
      "    \"PARTY_GID\": {\n" +
      "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"gid\"}]\n" +
      "    }, \n" +
      "    \"PATIENT_ID\": {\n" +
      "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"patient_id\"}]\n" +
      "    }\n" +
      "  }\n" +
      "}";

    List<FrameConfiguration> frameConfigurations = schemaAccessor
      .readMultipleConfiguration(new StringReader(configExtraMetaKey));

    assertThat(frameConfigurations, hasSize(1));
    assertThat(frameConfigurations,
      contains(
        hasToString(
          "FrameConfiguration{name='COS_PATIENT', partitioningColumn='PARTY_GID'}")));
  }

  @Test
  public void testMultipleConfigurationsDiscardsUnknownKeysInStrategiesBlock() {
    String extraStrategyKeys = "{\n" +
      "  \"COS_PATIENT\" : {\n" +
      "    \"_meta\": {\n" +
      "      \"partitioningColumn\": \"PARTY_GID\"\n" +
      "    }, \n" +
      "    \"PARTY_GID\": {\n" +
      "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"gid\"}]\n" +
      "    }, \n" +
      "    \"PATIENT_ID\": {\n" +
      "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"patient_id\"}]\n" +
      "    }\n" +
      "  }\n" +
      "}";


    List<FrameConfiguration> frameConfigurations = new SchemaAccessor()
      .readMultipleConfiguration(new StringReader(extraStrategyKeys));

    assertThat(frameConfigurations, hasSize(1));
    assertThat(frameConfigurations,
      contains(
        hasToString(
          "FrameConfiguration{name='COS_PATIENT', partitioningColumn='PARTY_GID'}")));
  }

  /**
  @Test
  public void testMultipleConfigurationsThrowsJsonForrmatExceptionOnBadFormat() {
    String badFormat = "{\n" +
      "  \"COS_PATIENT\" : {\n" +
      "    \"_meta\": [\n" +
      "      {\"partitioningColumn\": \"PARTY_GID\"}\n" +
      "    ], \n" +
      "    \"PARTY_GID\": {\n" +
      "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"gid\"}]\n" +
      "    }, \n" +
      "    \"PATIENT_ID\": {\n" +
      "      \"strategy\": [{\"type\": \"Hash\", \"salt_key\": \"patient_id\"}]\n" +
      "    }\n" +
      "  }\n" +
      "}";

    expectedException.expect(JsonSyntaxException.class);
    schemaAccessor.readMultipleConfiguration(new StringReader(badFormat));
  }

  */
}