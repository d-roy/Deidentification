package net.navinet.research.deidentification;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class PropertiesReaderTest {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetCommandLineArgsReturnsEmptyMapFromEmptyString() {
    Map<String, String> results = PropertiesReader.getCommandLineArgs(new String[] {});

    assertThat(results.keySet(), is(empty()));
  }

  @Test
  public void testGetCommandLineArgsThrowsExceptionWhenGivenIncorrectFormat() {
    expectedException.expect(IllegalArgumentException.class);
    PropertiesReader.getCommandLineArgs(new String[]{"/vagrant/input"});
  }

  @Test
  public void testGetCommandLineArgsBuildsMapOutOfFormattedCommandLineArguments() {
    Map<String, String> results = PropertiesReader.getCommandLineArgs(new String[]{
      "input=/vagrant/input.json", "schema=/vagrant/schema.json", "output=/vagrant/output.json"});

    assertThat(results.keySet(), containsInAnyOrder("input", "schema", "output"));
    assertThat(results.get("input"), is("/vagrant/input.json"));
    assertThat(results.get("schema"), is("/vagrant/schema.json"));
  }
}