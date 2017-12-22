package co.cask.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.util.List;
import java.util.Map;

/**
 * Class description here.
 */
public class DefaultProfiler extends Profiler {

  public DefaultProfiler(List<Profile> profiles, Schema schema) {
    super(profiles, schema);
  }

  @Override
  public void update(String name, Object value) {
    List<Profile> profiles = getProfiles(name);
    for(Profile profile : profiles) {
      profile.update(value);
    }
  }

  @Override
  public StructuredRecord result(String name) {
    StructuredRecord.Builder builder = StructuredRecord.builder(getOutputSchema());
    builder.set("name", name);
    for (Profile profile : getProfiles(name)) {
      StructuredRecord record = profile.results(
        Schema.recordOf(
          profile.name(),
          profile.fields()
        )
      );
      builder.set(profile.name(), record);
    }
    return builder.build();
  }
}
