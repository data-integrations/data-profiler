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
 * This class <code>Logical</code> profilers profiles boolean columns.
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
      Schema.Field.of("positive", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("negative", Schema.of(Schema.Type.LONG)),
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
