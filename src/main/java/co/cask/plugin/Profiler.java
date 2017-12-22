package co.cask.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Statistics about a field.
 */
public abstract class Profiler {
  private Map<String, List<Profile>> types = new TreeMap<>();
  private Map<String, List<Profile>> mappings;
  private Schema output, input;
  private List<Profile> profiles;

  public Profiler(List<Profile> profiles, Schema input) {
    this.input = input;
    this.profiles = profiles;
    this.output = createOutputSchema();
    if (input != null) {
      this.mappings = createMappings();
    }
  }

  private Map<String, List<Profile>> createMappings() {
    Map<String, List<Profile>> maps = new TreeMap<>();
    for (Schema.Field field : input.getFields()) {
      Schema schema = field.getSchema();
      if (!schema.isSimpleOrNullableSimple()) {
        continue;
      }
      if (schema.isNullable()) {
        schema = schema.getNonNullable();
      }
      String type = schema.getType().name();
      String name = field.getName();
      if (types.containsKey(type)) {
        maps.put(name, types.get(type));
      }
    }
    return maps;
  }

  public Schema getOutputSchema() {
    return output;
  }

  private Schema createOutputSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    for (Profile profile : profiles) {
      List<Schema.Field> results = profile.fields();
      fields.add(Schema.Field.of(
        profile.name(),
        Schema.nullableOf(
          Schema.recordOf(profile.name(), results)
        ))
      );

      for (Schema.Type type : profile.types()) {
        if (types.containsKey(type.name())) {
          List<Profile> temp = types.get(type.name());
          temp.add(profile);
          types.put(type.name(), temp);
        } else {
          List<Profile> temp = new ArrayList<>();
          temp.add(profile);
          types.put(type.name(), temp);
        }
      }
      profile.initialize();
    }
    return Schema.recordOf("profiles", fields);
  }

  public void reset() {
    for (Profile profile : profiles) {
      profile.reset();
    }
  }

  protected List<Profile> getProfiles(String name) {
    if (mappings.containsKey(name)) {
      return mappings.get(name);
    }
    return new ArrayList<>();
  }

  public abstract void update(String name, Object value);
  public abstract StructuredRecord result(String name);
}
