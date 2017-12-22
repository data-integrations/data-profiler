package co.cask.plugin.profiles;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.plugin.Profile;

import java.util.Arrays;
import java.util.List;

/**
 * Class description here.
 */
public final class Logical extends Profile {
  private long countTrue;
  private long countFalse;
  private long countUnknown;

  public Logical() {
    super("logical");
  }

  @Override
  public List<Schema.Type> types() {
    return Arrays.asList(Schema.Type.BOOLEAN);
  }

  @Override
  public List<Schema.Field> fields() {
    return Arrays.asList(
      Schema.Field.of("true", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("false", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("unknown", Schema.of(Schema.Type.LONG))
    );
  }

  @Override
  public void reset() {
    this.countTrue = 0;
    this.countFalse = 0;
    this.countUnknown = 0;
  }

  @Override
  public void update(Object value) {
    if (value instanceof Boolean) {
      if (value == null) {
        countUnknown++;
      } else {
        boolean val = (Boolean) value;
        if (val) {
          countTrue++;
        } else {
          countFalse++;
        }
      }
    }
  }

  @Override
  public void results(StructuredRecord.Builder builder) {
    builder.set("true", countTrue);
    builder.set("false", countFalse);
    builder.set("unknown", countUnknown);
  }
}
