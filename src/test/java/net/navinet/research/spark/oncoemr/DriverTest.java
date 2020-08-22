package net.navinet.research.spark.oncoemr;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import net.navinet.research.deidentification.schema.FrameConfiguration;
import net.navinet.research.deidentification.schema.SchemaAccessor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DriverTest {
  @Test
  public void testGetConfigElementsSuccessfullyLoadsDeidFile() {
    SchemaAccessor schemaAccessor = new SchemaAccessor();
    List<FrameConfiguration> configs = Driver.getConfigElements(schemaAccessor);

    List<String> names = new ArrayList<>();
    for (FrameConfiguration config : configs) {
      names.add(config.name());
    }

    for (String tableName : Driver.names) {
      assertThat(names, hasItem(tableName));
    }
  }
}