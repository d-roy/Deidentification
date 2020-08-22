package net.navinet.research.deidentification.schema;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import net.navinet.research.deidentification.deidentify.ColumnStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.*;


/**
 * Handler for reading configuration files
 */
public final class SchemaAccessor {
  private static final Logger logger = LogManager.getLogger(SchemaAccessor.class);
  private static final Gson gson = new Gson();


  public List<FrameConfiguration> readMultipleConfiguration(Reader configurationReader) {
    Objects.requireNonNull(configurationReader);

    Map<String, TableElement> tables;
    try (JsonReader jsonReader = new JsonReader(configurationReader)){
      tables = parseMultipleConfigFile(jsonReader);
    } catch (IOException | IllegalStateException iex) {
      JsonSyntaxException exn = new JsonSyntaxException(iex);
      logger.error("Failed to correctly parse database-inbound config format.", exn);
      throw exn;
    }


    List<FrameConfiguration> frameConfigurations = new ArrayList<>();
    for (Map.Entry<String, TableElement> table : tables.entrySet()) {
      List<ColumnStrategy> strategies = this.<
        ArrayList<ColumnStrategySchemaImpl>,
        HashMap<String, ArrayList<ColumnStrategySchemaImpl>>,
        HashMap<String, HashMap<String, ArrayList<ColumnStrategySchemaImpl>>>
        >parseFormatToColumnStrategies(table.getValue().columns);

      String tableName = table.getKey();
      String partitioningColumnName = table.getValue().partitioningColumn;

      frameConfigurations.add(
        new FrameConfigurationImpl(tableName, partitioningColumnName, strategies));
    }

    return frameConfigurations;
  }

  public List<ColumnStrategy> readSingleConfiguration(Reader configurationReader) {
    Objects.requireNonNull(configurationReader);

    Type schema = new TypeToken<Map<String, Map<String, List<ColumnStrategySchemaImpl>>>>() {

    }.getType();

    Map<String, Map<String, List<ColumnStrategySchemaImpl>>> unformattedStrategies = gson
      .fromJson(configurationReader, schema);

    return this.parseFormatToColumnStrategies(unformattedStrategies);
  }

  private <
    StratList extends List<ColumnStrategySchemaImpl>,
    InnerMap extends Map<String, StratList>,
    OuterMap extends Map<String, InnerMap>
    > List<ColumnStrategy> parseFormatToColumnStrategies(OuterMap fromFile) {
    List<ColumnStrategy> strategies = new ArrayList<>();

    for (Map.Entry<String, InnerMap> entry : fromFile.entrySet()) {
      for (Map.Entry<String, StratList> column : entry.getValue().entrySet()) {
        for (ColumnStrategySchemaImpl schema : column.getValue()) {
          schema.setColumnName(entry.getKey());
          schema.setSaltKey(entry.getKey());
          strategies.add(schema);
        }
      }
    }

    return strategies;
  }


  private Map<String, TableElement> parseMultipleConfigFile(JsonReader reader)
    throws IOException {
    Map<String, TableElement> tables = new HashMap<>();

    reader.beginObject();
    while (reader.hasNext()) {
      String tableName = reader.nextName();
      TableElement tableElement = parseTableElement(reader);
      tables.put(tableName, tableElement);
    }
    reader.endObject();

    return tables;
  }

  private TableElement parseTableElement(JsonReader reader) throws IOException {
    Map<String, String> metaInformation = new HashMap<>();
    HashMap<String, HashMap<String, ArrayList<ColumnStrategySchemaImpl>>> columnStrategies
      = new HashMap<>();

    reader.beginObject();
    while (reader.hasNext()) {
      String nextName = reader.nextName();
      if (nextName.equals("_meta")) {
        metaInformation = getMetaInformation(reader);
      } else {
        columnStrategies.put(nextName, parseColumnStrategy(reader));
      }
    }
    reader.endObject();

    String partitioningColumn = metaInformation.get("partitioningColumn");
    return new TableElement(partitioningColumn, columnStrategies);
  }

  /**
   * NULLABLE. Expects to start OVER AN OBJECT.
   *
   * @param reader Current JSON reader. Expects to be over an object
   * @return Name of the column to be used for partitioning
   * @throws IOException If not over an object, or due to other formatting problems.
   */
  private Map<String, String> getMetaInformation(JsonReader reader) throws IOException {
    Map<String, String> meta = new HashMap<>();

    reader.beginObject();
    while (reader.hasNext()) {
      String key = reader.nextName();
      if (key.equals("partitioningColumn")) {
        meta.put(key, reader.nextString());
      } else {
        // I don't really care about any other keys. They will be read and skipped,
        // because compatibility?
        reader.skipValue();
      }
    }
    reader.endObject();

    return meta;
  }

  private HashMap<String, ArrayList<ColumnStrategySchemaImpl>> parseColumnStrategy(JsonReader reader)
    throws IOException {
    HashMap<String, ArrayList<ColumnStrategySchemaImpl>> strategies = new HashMap<>();

    reader.beginObject();
    while (reader.hasNext()) {
      String key = reader.nextName();
      if (key.equals("strategy")) {
        Type strategiesType = new TypeToken<ArrayList<ColumnStrategySchemaImpl>>() {

        }.getType();

        ArrayList<ColumnStrategySchemaImpl> strategySchemas = gson.fromJson(reader, strategiesType);
        strategies.put("strategy", strategySchemas);
      } else {
        // It may have other keys. These other keys should be skipped, with an eye on possible
        // future extensions to the model.
        reader.skipValue();
      }
    }
    reader.endObject();

    return strategies;
  }


  private static final class ColumnStrategySchemaImpl implements ColumnStrategy {
    private String name;
    private String type;
    private String salt_key;
    private String date_epoch;

    @Override
    public String getColumnName() {
      return this.name;
    }

    public void setColumnName(String columnName) {
      this.name = columnName;
    }

    public void setSaltKey(String saltKey) { this.salt_key = saltKey; }

    @Override
    public String getType() {
      return this.type;
    }

    @Override
    public String saltKey() {
      return this.salt_key;
    }

    @Override
    public String dateEpoch() {
      return this.date_epoch;
    }

    @Override
    public String toString() {
      return "ColumnStrategySchemaImpl{" +
        "name='" + name + '\'' +
        ", type='" + type + '\'' +
        '}';
    }
  }

  private static final class TableElement {
    String partitioningColumn; // Nullable
    HashMap<String, HashMap<String, ArrayList<ColumnStrategySchemaImpl>>> columns;

    TableElement(
      String partitioningColumn,
      HashMap<String, HashMap<String, ArrayList<ColumnStrategySchemaImpl>>> columns) {
      this.partitioningColumn = partitioningColumn;
      this.columns = columns;
    }
  }
}
