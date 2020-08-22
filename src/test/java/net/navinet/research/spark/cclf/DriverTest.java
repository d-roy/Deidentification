package net.navinet.research.spark.cclf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;

import net.navinet.research.deidentification.schema.FrameConfiguration;
import net.navinet.research.deidentification.schema.SchemaAccessor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
/*
import java.nio.file.Path;
import java.nio.file.Paths;
*/

import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.List;


public class DriverTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testCanAccessAllSchemaFiles() throws IOException {
    for (int i = 1; i <= 9; i++) {
      InputStream input = this.getClass()
          .getClassLoader()
          .getResourceAsStream(
              Driver.getSchemaPathFromFileName("cclf" + i));

      assertThat(input, is(notNullValue()));
      input.close();
    }
  }

  @Test
  public void testGetSchemaFileThrowsExnOnBadFileName() {
    expectedException.expect(IllegalArgumentException.class);
    Driver.getSchemaPathFromFileName("NotAReal.file");
  }

  @Test
  public void testGetConfigElementsSuccessfullyLoadsDeidFile() {
    SchemaAccessor schemaAccessor = new SchemaAccessor();
    List<FrameConfiguration> configs = Driver.getConfigElements(schemaAccessor);

    assertThat(configs, hasSize(9));
    List<String> names = new ArrayList<>();

    for (FrameConfiguration config : configs) {
      names.add(config.name());
    }

    for (int i = 1; i <= 9; i += 1) {
      assertThat(names, hasItem("cclf" + i));
    }
  }


  @Test
  public void testGetMatchingConfigElementMatchesKnownFilesSuccessfully() {
    SchemaAccessor schemaAccessor = new SchemaAccessor();
    List<FrameConfiguration> configs = Driver.getConfigElements(schemaAccessor);

    String[] cclfFiles = {
      "path/to/cclf1_part_a_clmhdr.fil",
      "path/to/cclf2_part_a_revclmdet.fil",
      "path/to/cclf3_part_a_proc_code.fil",
      "path/to/cclf4_part_a_diag_code.fil",
      "path/to/cclf5_part_b_provider.fil",
      "path/to/cclf6_part_b_dme.fil",
      "path/to/cclf7_part_d_claim.fil",
      "path/to/cclf8_member_demog_fil",
      "path/to/cclf9_member_demog_xref.fil"
    };

    for (String cclfFile : cclfFiles) {
      Path cclfPath = new Path(cclfFile);
      FrameConfiguration config = Driver.getMatchingFrameConfig(cclfPath, configs);

      assertThat(config, is(notNullValue()));
      assertTrue(config.name().matches("cclf\\d"));
    }
  }

}