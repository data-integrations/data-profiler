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
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.plugin.Profile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class description here.
 */
public final class Histogram extends Profile {
  private static final Logger LOG = LoggerFactory.getLogger(Histogram.class);
  private DynamicHistogram histogram;

  private Schema histogramSchema = Schema.recordOf(
    "histrec",
    Schema.Field.of("low", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("high", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("count", Schema.of(Schema.Type.DOUBLE))
  );

  public Histogram() {
    super("histogram");
  }

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

  @Override
  public List<Schema.Field> fields() {
    Schema.Field field = Schema.Field.of("hist", Schema.nullableOf(Schema.arrayOf(histogramSchema)));
    return Arrays.asList(field);
  }

  @Override
  public void reset() {
    histogram = new DynamicHistogram(10, 5, 10);
  }

  @Override
  public void update(Object value) {
    if (value != null) {
      double val = 0;
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
      } else {
        return;
      }
      histogram.addDataPoint(val);
    }
  }

  @Override
  public void results(StructuredRecord.Builder builder) {
    DynamicHistogram.Bucket[] buckets = histogram.getHistogram();
    if (buckets != null && buckets.length > 0) {
      double high = 0;
      List<StructuredRecord> points = new ArrayList<>();
      for (int i = 0; i < buckets.length; ++i) {
        double low = high;
        DynamicHistogram.Bucket bucket = buckets[i];
        high = bucket.getHigh();

        StructuredRecord.Builder hist = StructuredRecord.builder(histogramSchema);
        hist.set("low", low);
        hist.set("high", high);
        hist.set("count", bucket.getCount());
        StructuredRecord point = hist.build();
        points.add(point);
        try {
          String s = StructuredRecordStringConverter.toJsonString(point);
          LOG.info("Structured Record {}", s);
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
      builder.set("hist", points);
    }
  }
}
