package net.navinet.research.spark.cclf.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by KAllen on 6/16/2016.
 */
public class FixedWidthParserTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParse() {
    JsonElement root = new JsonParser().parse("[{\"name\":\"5chars\",\"length\":5}, {\"name\":\"2chars\",\"length\":2}, {\"name\":\"9chars\",\"length\":9}]");
    Gson g = new Gson();
    List<Integer> x = new ArrayList<>();
    for (JsonElement j : root.getAsJsonArray()) {
      x.add(j.getAsJsonObject().get("length").getAsInt());
    }
    assertEquals(g.toJson(FixedWidthParser.flatParse(x, "012345678910111213141516171819202122232425")), "[\"01234\",\"56\",\"789101112\"]");
    assertEquals(g.toJson(FixedWidthParser.flatParse(x, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")), "[\"ABCDE\",\"FG\",\"HIJKLMNOP\"]");
    x.clear();
    x.add(2);
    assertArrayEquals(FixedWidthParser.flatParse(x, "1234").toArray(), new String[]{"12"});
    x.add(4);
    assertArrayEquals(FixedWidthParser.flatParse(x, "1234ABCD").toArray(), new String[]{"12", "34AB"});
    x.add(7);
    assertArrayEquals(FixedWidthParser.flatParse(x, "1234ABCDEFGHI").toArray(), new String[]{"12", "34AB", "CDEFGHI"});
  }

  @Test
  public void testFlatParseThrowsIllegalArgumentExceptionOnShortString() {
    List<Integer> partitions = new ArrayList<>();
    partitions.add(2);
    partitions.add(3);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected string of length at least 5; got one of length 4");
    FixedWidthParser.flatParse(partitions, "abcd");
  }

  @Test
  public void testFlatParseReturnsEmptyArrayWhenGivenEmptyPartitions() {
    List<Integer> partitions = new ArrayList<>();

    List<String> output = FixedWidthParser.flatParse(partitions, "abcd");

    assertThat(output, is(empty()));
  }

  @Test
  public void testParseWithSpacesInElements() {
    // Broken up at random, but this (should be) a legitimate input string
    String input = "9999999999999" + "     0" + "          0" + "00" + "1995-10-21";

    List<Integer> partitions = new ArrayList<>();
    partitions.add(13);
    partitions.add(6);
    partitions.add(11);
    partitions.add(2);
    partitions.add(10);

    List<String> output = FixedWidthParser.flatParse(partitions, input);

    assertThat(output, hasSize(5));
    assertThat(output.get(0), is("9999999999999"));
    assertThat(output.get(1), is("     0"));
    assertThat(output.get(2), is("          0"));
    assertThat(output.get(3), is("00"));
    assertThat(output.get(4), is("1995-10-21"));
  }
}