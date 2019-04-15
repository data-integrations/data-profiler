/*
 * Copyright © 2018-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.List;

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
