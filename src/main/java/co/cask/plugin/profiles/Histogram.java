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
    "histogram",
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
    return Arrays.asList(
      Schema.Field.of("histogram", Schema.nullableOf(Schema.arrayOf(histogramSchema)))
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
      builder.set("histogram", points);
    }
  }
}
