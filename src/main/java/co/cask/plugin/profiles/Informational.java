package co.cask.plugin.profiles;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.plugin.Profile;

import java.util.Arrays;
import java.util.List;

/**
 * Class description here.
 */
public final class Informational extends Profile {

  public Informational() {
    super("informational");
  }

  @Override
  public List<Schema.Type> types() {
    return Arrays.asList(
      Schema.Type.STRING
    );
  }

  @Override
  public List<Schema.Field> fields() {
    List<Schema.Field> fields = Arrays.asList(
      Schema.Field.of("type", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("mean", Schema.of(Schema.Type.DOUBLE))
    );
    return Arrays.asList(
      Schema.Field.of("types", Schema.nullableOf(Schema.recordOf("stats", fields)))
    );
  }

  @Override
  public void reset() {
  }

  @Override
  public void update(Object value) {

  }

  @Override
  public void results(StructuredRecord.Builder builder) {
  }
}
