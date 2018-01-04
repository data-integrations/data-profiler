/*
 * Copyright © 2018 Cask Data, Inc.
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
