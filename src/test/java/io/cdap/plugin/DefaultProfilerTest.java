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
import io.cdap.plugin.profiles.Categorical;
import io.cdap.plugin.profiles.DynamicHistogram;
import io.cdap.plugin.profiles.Histogram;
import io.cdap.plugin.profiles.Logical;
import io.cdap.plugin.profiles.Quantitative;
import io.cdap.plugin.profiles.Uniques;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public class DefaultProfilerTest {

  private Schema schema = Schema.recordOf(
    "data",
    Schema.Field.of("s", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("i", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("l", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("f", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
    Schema.Field.of("d", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

  @Test
  public void testBasicFunctionality() throws Exception {
    List<Profile> profiles = new ArrayList<>();
    profiles.add(new Categorical());
    profiles.add(new Histogram());
    profiles.add(new Logical());
    profiles.add(new Uniques());
    profiles.add(new Quantitative());

    Profiler profiler = new DefaultProfiler(profiles, schema);
    Schema outputSchema = profiler.getOutputSchema();
    profiler.update("s", "1");
    StructuredRecord result = profiler.result("s");
    Assert.assertNotNull(result);
    Assert.assertNotNull(outputSchema);
  }

  @Test
  public void testHyperLogLog() throws Exception {
    HyperLogLogPlus hll = new HyperLogLogPlus(32, 32);
    hll.offer("a");
    hll.offer("b");
    hll.offer("c");
    hll.offer("c");
    Assert.assertEquals(3, hll.cardinality());
  }

  @Test
  public void testDynamicHistogram() throws Exception {
    DynamicHistogram histogram = new DynamicHistogram(20, 5, 1000);
    for (double d = 0; d < 10; d+=0.001) {
      histogram.addDataPoint(d);
    }
    DynamicHistogram.Bucket[] buckets = histogram.getHistogram();
    Assert.assertTrue(buckets.length > 0);
  }



}
