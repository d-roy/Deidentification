package net.navinet.research.spark.cclf.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by KAllen on 6/16/2016.
 */
final class FixedWidthParser {

  private FixedWidthParser() {
  }

  /**
   * Parses a string into chunks based on the lengths in the given list,
   * i.e. [1,2,3],"123456" -> ["1", "23", "456"]
   *
   * @param chunkSizes the sequence of sizes to break into
   * @param data       the string to divide up
   * @return a list containing the divided parts of the string
   * @throws IllegalArgumentException if data does not contain enough characters
   *                                  to break into the chunks given by chunkSizes
   */
  static List<String> flatParse(Iterable<Integer> chunkSizes, String data) {
    int index = 0;
    List<String> fin = new ArrayList<>();
    for (int chunkLength : chunkSizes) {
      if (index + chunkLength > data.length()) {
        throw new IllegalArgumentException(
            String.format("Expected string of length at least %d; got one of length %d",
                index + chunkLength, data.length()));
      }
      fin.add(data.substring(index, index + chunkLength));
      index += chunkLength;
    }
    return fin;
  }
}
