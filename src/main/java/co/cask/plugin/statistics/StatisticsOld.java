package co.cask.plugin.statistics;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

/**
 * Class description here.
 */
public class StatisticsOld {
  public static final Schema SCHEMA = Schema.recordOf(
    "fieldStats",
    Schema.Field.of("field", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("total_count", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("null_count", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("zero_count", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("pos_count", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("neg_count", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("minimum", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("maximum", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("mean", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("stddev", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("empty_count", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("len_min", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("len_max", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("len_mean", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("len_stddev", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("true_count", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
    Schema.Field.of("false_count", Schema.nullableOf(Schema.of(Schema.Type.LONG))));
  private double min;
  private double max;
  private double sum;
  private int minLen;
  private int maxLen;
  private long sumLen;
  private long count;
  private long nullCount;
  private long zeroCount;
  private long posCount;
  private long negCount;
  private long emptyCount;
  private long trueCount;
  private long falseCount;
  private boolean hasBool;
  private boolean hasNumeric;
  private boolean hasString;

  public StatisticsOld() {
    reset();
  }

  public void reset() {
    min = Double.POSITIVE_INFINITY;
    max = Double.NEGATIVE_INFINITY;
    sum = 0d;
    minLen = Integer.MAX_VALUE;
    maxLen = Integer.MIN_VALUE;
    sumLen = 0L;
    count = 0L;
    nullCount = 0L;
    zeroCount = 0L;
    posCount = 0L;
    negCount = 0L;
    emptyCount = 0L;
    trueCount = 0L;
    falseCount = 0L;
    hasBool = false;
    hasNumeric = false;
    hasString = false;
  }

  public void update(Schema.Type type, Object val) {
    count++;
    if (val == null) {
      nullCount++;
      return;
    }

    switch (type) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double doubleVal = ((Number) val).doubleValue();
        if (doubleVal == 0d) {
          zeroCount++;
        } else if (doubleVal < 0d) {
          negCount++;
        } else if (doubleVal > 0d) {
          posCount++;
        }
        sum += doubleVal;
        min = Math.min(doubleVal, min);
        max = Math.max(doubleVal, max);
        hasNumeric = true;
        break;
      case STRING:
        String strVal = (String) val;
        int strLen = strVal.length();
        sumLen += strLen;
        if (strLen == 0) {
          emptyCount++;
        }
        minLen = Math.min(strLen, minLen);
        maxLen = Math.max(strLen, maxLen);
        hasString = true;
        break;
      case BOOLEAN:
        if ((Boolean) val) {
          trueCount++;
        } else {
          falseCount++;
        }
        hasBool = true;
        break;
    }
  }

  public double getMin() {
    return min;
  }

  public double getMax() {
    return max;
  }

  public double getMean() {
    return sum / (count - nullCount);
  }

  public int getMinLen() {
    return minLen;
  }

  public int getMaxLen() {
    return maxLen;
  }

  public double getMeanLen() {
    return (double) sumLen / (count - nullCount);
  }

  public long getCount() {
    return count;
  }

  public long getNullCount() {
    return nullCount;
  }

  public long getZeroCount() {
    return zeroCount;
  }

  public long getPosCount() {
    return posCount;
  }

  public long getNegCount() {
    return negCount;
  }

  public long getEmptyCount() {
    return emptyCount;
  }

  public long getTrueCount() {
    return trueCount;
  }

  public long getFalseCount() {
    return falseCount;
  }

  public StructuredRecord toRecord(String fieldName) {
    StructuredRecord.Builder output = StructuredRecord.builder(SCHEMA)
      .set("field", fieldName)
      .set("total_count", count)
      .set("null_count", nullCount);
    if (hasBool) {
      output.set("true_count", trueCount);
      output.set("false_count", falseCount);
    }
    long nonNullCount = count - nullCount;
    if (hasNumeric) {
      output.set("minimum", min);
      output.set("maximum", max);
      output.set("mean", sum / nonNullCount);
      output.set("zero_count", zeroCount);
      output.set("pos_count", posCount);
      output.set("neg_count", negCount);
    }
    if (hasString) {
      output.set("len_min", minLen);
      output.set("len_max", maxLen);
      output.set("len_mean", (double) sumLen / nonNullCount);
      output.set("empty_count", emptyCount);
    }
    return output.build();
  }
}
