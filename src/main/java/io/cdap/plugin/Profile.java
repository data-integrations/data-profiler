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
