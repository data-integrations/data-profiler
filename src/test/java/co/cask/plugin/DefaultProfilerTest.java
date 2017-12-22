package co.cask.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.plugin.profiles.Categorical;
import co.cask.plugin.profiles.DynamicHistogram;
import co.cask.plugin.profiles.Informational;
import co.cask.plugin.profiles.Logical;
import co.cask.plugin.profiles.Quantitative;
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
    profiles.add(new Informational());
    profiles.add(new Logical());
    profiles.add(new Quantitative());

    Profiler profiler = new DefaultProfiler(profiles, schema);
    profiler.update("s", "1");
    StructuredRecord result = profiler.result("s");
    Assert.assertNotNull(result);
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