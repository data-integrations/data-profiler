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

package io.cdap.plugin.profiles;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.Profile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class <code>Historgram</code> profiler generates histogram for values of
 * types -- integer, long, float, double and string. For string, the histogram is based
 * on the length of the string.
 *
 * This class generates 10 buckets and the buckets are dynamically created as the values
 * are being added to the profiler.
 *
 * The resulting record contains -- low, high and count for each bucket.
 */
public final class Histogram extends Profile {
  private static final String LOW = "low";
  private static final String HIGH = "high";
  private static final String COUNT = "count";
  private static final String RECORD = "hist";
  private static final int NUMBER_OF_BUCKETS = 10;
  private static final int INITIAL_DATAPOINTS_PER_BUCKET = 5;
  private static final int HALF_LIFE = 10;

  private DynamicHistogram histogram;

  // Schema for each bucket.
  private Schema schema = Schema.recordOf(
    "histrec",
    Schema.Field.of(LOW, Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of(HIGH, Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of(COUNT, Schema.of(Schema.Type.DOUBLE))
  );

  public Histogram() {
    super("histogram");
  }

  /**
   * This profiler is responsible for handling INT, LONG, FLOAT, DOUBLE and STRING types.
   *
   * @return List of types supported by this profiler.
   */
  @Override
  public List<Schema.Type> types() {
    return Arrays.asList(
      Schema.Type.INT,
      Schema.Type.LONG,
      Schema.Type.FLOAT,
      Schema.Type.DOUBLE,
      Schema.Type.STRING
    );
  }

  /**
   * Specifies the schema that this profiler would use to communicate the results of
   * histogram.
   *
   * @return Schema associated with histogram results.
   */
  @Override
  public List<Schema.Field> fields() {
    return Arrays.asList(
      Schema.Field.of(
        RECORD, Schema.nullableOf(
          Schema.arrayOf(schema)
        )
      )
    );
  }

  /**
   * Resets the internal states of histogram.
   */
  @Override
  public void reset() {
    histogram = new DynamicHistogram(
      NUMBER_OF_BUCKETS,
      INITIAL_DATAPOINTS_PER_BUCKET,
      HALF_LIFE
    );
  }

  /**
   * Updates the internal states of histogram.
   *
   * @param value to be used to update histogram.
   */
  @Override
  public void update(Object value) {
    if (value != null) {
      double val = 0.0;
      if (value instanceof Integer) {
        val = Double.valueOf((Integer) value).doubleValue();
      } else if (value instanceof Long) {
        val = Double.valueOf((Long) value).doubleValue();
      } else if (value instanceof Float) {
        val = Double.valueOf((Float) value).doubleValue();
      } else if (value instanceof Double) {
        val = (Double) value;
      } else if (value instanceof String) {
        val = ((String) value).length();
      } else if (value instanceof Short) {
        val = Double.valueOf((Short) value ).doubleValue();
      } else {
        return;
      }
      histogram.addDataPoint(val);
    }
  }

  /**
   * Adds all the internal states of buckets into a <code>StructuredRecord</code>.
   *
   * @param builder to add the internal states of the record.
   */
  @Override
  public void results(StructuredRecord.Builder builder) {
    DynamicHistogram.Bucket[] buckets = histogram.getHistogram();
    if (buckets != null && buckets.length > 0) {
      double high = 0;
      List<StructuredRecord> points = new ArrayList<>();
      for (int i = 0; i < buckets.length; ++i) {
        DynamicHistogram.Bucket bucket = buckets[i];
        double low = high;
        high = bucket.getHigh();
        StructuredRecord.Builder hist = StructuredRecord.builder(schema);
        hist.set(LOW, low);
        hist.set(HIGH, high);
        hist.set(COUNT, bucket.getCount());
        points.add(hist.build());
      }
      builder.set(RECORD, points);
    }
  }
}
