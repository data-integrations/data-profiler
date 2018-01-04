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
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;
import java.util.List;

/**
 * Class description here.
 */
public final class Categorical extends Profile {
  private DescriptiveStatistics statistics;
  private long count;
  private long nulls;
  private long empty;

  public Categorical() {
    super("categorical");
    statistics = new DescriptiveStatistics();
  }

  @Override
  public List<Schema.Type> types() {
    return Arrays.asList(
      Schema.Type.STRING
    );
  }

  @Override
  public List<Schema.Field> fields() {
    return Arrays.asList(
      Schema.Field.of("min", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("max", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("empty", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("mean", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("stdev", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("median", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("skewness", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("kurtosis", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("population_variance", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("nulls", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("non_nulls", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("geometric_mean", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("quadratic_mean", Schema.of(Schema.Type.DOUBLE))
    );
  }

  @Override
  public void reset() {
    statistics.clear();
    count = 0;
    nulls = 0;
    empty = 0;
  }

  @Override
  public void update(Object value) {
    count = count + 1;
    if (value instanceof String) {
      if (value == null) {
        nulls = nulls + 1;
      } else {
        String val = (String) value;
        if (val.isEmpty()) {
          empty = empty + 1;
        } else {
          statistics.addValue(val.length());
        }
      }
    }
  }


  @Override
  public void results(StructuredRecord.Builder builder) {
    builder.set("nulls", nulls);
    builder.set("non_nulls", count - nulls);
    builder.set("empty", empty);
    builder.set("max", V(statistics.getMax()));
    builder.set("min", V(statistics.getMin()));
    builder.set("mean", V(statistics.getMean()));
    builder.set("stdev", V(statistics.getStandardDeviation()));
    builder.set("median", V(statistics.getPercentile(50)));
    builder.set("geometric_mean", V(statistics.getGeometricMean()));
    builder.set("skewness", V(statistics.getSkewness()));
    builder.set("kurtosis", V(statistics.getKurtosis()));
    builder.set("population_variance", V(statistics.getPopulationVariance()));
    builder.set("quadratic_mean", V(statistics.getQuadraticMean()));
  }
}
