package co.cask.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.util.List;

/**
 * Class description here.
 */
public abstract class Profile {
  private String name;
  public Profile(String name) {
    this.name = name;
  }

  public void initialize() {
    reset();
  }

  public abstract List<Schema.Type> types();
  public String name() {
    return name;
  }
  public abstract List<Schema.Field> fields();
  public abstract void reset();
  public abstract void update(Object value);

  public StructuredRecord results(Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    results(builder);
    return builder.build();
  }

  protected double V(double value) {
    if(value != value) {
      return 0;
    }
    return value;
  }

  public abstract void results(StructuredRecord.Builder builder);
}
