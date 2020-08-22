package net.navinet.research.deidentification;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public final class PropertiesReader {
  private static final Logger logger = LogManager.getLogger(PropertiesReader.class);

  public static Map<String, String> getCommandLineArgs(String[] args) {
    Map<String, String> settings = new HashMap<>();

    for (String arg : args) {
      String[] splits = arg.split("=");
      if (splits.length != 2) {
        logger.error("Bad format in argument " + arg + ". Expected format: k=v");
        throw new IllegalArgumentException("Bad format in argument " + arg
          + ". Expected format: k=v");
      }
      settings.put(splits[0].trim(), splits[1].trim());
    }

    return settings;
  }
}
