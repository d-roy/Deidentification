package net.navinet.research.deidentification.connection;

import com.google.gson.Gson;
import org.apache.spark.sql.SQLContext;

import java.io.Reader;
import java.util.Objects;

/**
 * Factory for producing connections to a database or other sources of file input and output, such
 * as flat files.
 */
public final class ConnectionFactory {
  private final SQLContext sqlContext;
  private final Gson gson = new Gson();

  public ConnectionFactory(SQLContext sqlContext) {
    Objects.requireNonNull(sqlContext);
    this.sqlContext = sqlContext;
  }

  /**
   * Connect to the data source described in the configuration file. The configuration file to which
   * this function is intended to apply is
   * <p>
   * <code>
   * {
   * "connection_url": "jdbc:mysql://localhost:3306/COS_DB",
   * "username": "foo",
   * "password": "bar"
   * }
   * </code>
   *
   * @param reader Connection configuration
   * @return Connection to data source
   */
  public <T extends Connection> T fromConfiguration(Reader reader) {
    Objects.requireNonNull(reader);
    return this.gson
        .fromJson(reader, ConnectionIntermediate.class)
        .toConnection(this.sqlContext);
  }

  /**
   * Target class used by GSON for de-serialization
   */
  private static class ConnectionIntermediate {
    private String connection_url;
    private String username;
    private String password;
    private String format;

    // TODO This is total garbage and needs to be phased out.
    <T extends Connection> T toConnection(SQLContext sqlContext) {
      if (this.format == null) {
        return (T) new JDBCConnection(sqlContext, this.connection_url,
          this.username, this.password);
      }

      switch (this.format) {
        case "jdbc":
          return (T) new JDBCConnection(sqlContext, this.connection_url, this.username, this.password);
        default:
          throw new IllegalArgumentException("Unexpected connection type.");
      }
    }
  }
}
