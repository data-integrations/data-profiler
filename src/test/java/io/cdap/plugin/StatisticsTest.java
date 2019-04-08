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

package io.cdap.plugin;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.statistics.StatisticsOld;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Statistics.
 */
public class StatisticsTest {
  private static final Double ERROR = 0.000001d;

  @Test
  public void testString() {
    StatisticsOld stats = new StatisticsOld();
    stats.update(Schema.Type.STRING, "abc");
    stats.update(Schema.Type.STRING, "efg");
    stats.update(Schema.Type.STRING, "");
    stats.update(Schema.Type.STRING, null);

    Assert.assertEquals(4L, stats.getCount());
    Assert.assertEquals(1L, stats.getNullCount());
    Assert.assertEquals(1L, stats.getEmptyCount());
    Assert.assertEquals(0, stats.getMinLen());
    Assert.assertEquals(3, stats.getMaxLen());
    Assert.assertEquals(2d, stats.getMeanLen(), ERROR);
  }

  @Test
  public void testBool() {
    StatisticsOld stats = new StatisticsOld();
    stats.update(Schema.Type.BOOLEAN, true);
    stats.update(Schema.Type.BOOLEAN, false);
    stats.update(Schema.Type.BOOLEAN, false);
    stats.update(Schema.Type.BOOLEAN, null);
    stats.update(Schema.Type.BOOLEAN, null);

    Assert.assertEquals(5L, stats.getCount());
    Assert.assertEquals(2L, stats.getNullCount());
    Assert.assertEquals(1L, stats.getTrueCount());
    Assert.assertEquals(2L, stats.getFalseCount());
  }

  @Test
  public void testInt() {
    StatisticsOld stats = new StatisticsOld();
    stats.update(Schema.Type.INT, 0);
    stats.update(Schema.Type.INT, 10);
    stats.update(Schema.Type.INT, -10);
    stats.update(Schema.Type.INT, 100);
    stats.update(Schema.Type.INT, null);
    stats.update(Schema.Type.INT, null);

    Assert.assertEquals(6L, stats.getCount());
    Assert.assertEquals(2L, stats.getNullCount());
    Assert.assertEquals(1L, stats.getZeroCount());
    Assert.assertEquals(2L, stats.getPosCount());
    Assert.assertEquals(1L, stats.getNegCount());
    Assert.assertEquals(-10d, stats.getMin(), ERROR);
    Assert.assertEquals(100d, stats.getMax(), ERROR);
    Assert.assertEquals(25d, stats.getMean(), ERROR);
  }

  @Test
  public void testLong() {
    StatisticsOld stats = new StatisticsOld();
    stats.update(Schema.Type.LONG, 0L);
    stats.update(Schema.Type.LONG, 10L);
    stats.update(Schema.Type.LONG, -10L);
    stats.update(Schema.Type.LONG, 100L);
    stats.update(Schema.Type.LONG, null);
    stats.update(Schema.Type.LONG, null);

    Assert.assertEquals(6L, stats.getCount());
    Assert.assertEquals(2L, stats.getNullCount());
    Assert.assertEquals(1L, stats.getZeroCount());
    Assert.assertEquals(2L, stats.getPosCount());
    Assert.assertEquals(1L, stats.getNegCount());
    Assert.assertEquals(-10d, stats.getMin(), ERROR);
    Assert.assertEquals(100d, stats.getMax(), ERROR);
    Assert.assertEquals(25d, stats.getMean(), ERROR);
  }

  @Test
  public void testFloat() {
    StatisticsOld stats = new StatisticsOld();
    stats.update(Schema.Type.FLOAT, 0f);
    stats.update(Schema.Type.FLOAT, 10f);
    stats.update(Schema.Type.FLOAT, -10f);
    stats.update(Schema.Type.FLOAT, 100f);
    stats.update(Schema.Type.FLOAT, null);
    stats.update(Schema.Type.FLOAT, null);

    Assert.assertEquals(6L, stats.getCount());
    Assert.assertEquals(2L, stats.getNullCount());
    Assert.assertEquals(1L, stats.getZeroCount());
    Assert.assertEquals(2L, stats.getPosCount());
    Assert.assertEquals(1L, stats.getNegCount());
    Assert.assertEquals(-10d, stats.getMin(), ERROR);
    Assert.assertEquals(100d, stats.getMax(), ERROR);
    Assert.assertEquals(25d, stats.getMean(), ERROR);
  }

  @Test
  public void testDouble() {
    StatisticsOld stats = new StatisticsOld();
    stats.update(Schema.Type.DOUBLE, 0d);
    stats.update(Schema.Type.DOUBLE, 10d);
    stats.update(Schema.Type.DOUBLE, -10d);
    stats.update(Schema.Type.DOUBLE, 100d);
    stats.update(Schema.Type.DOUBLE, null);
    stats.update(Schema.Type.DOUBLE, null);

    Assert.assertEquals(6L, stats.getCount());
    Assert.assertEquals(2L, stats.getNullCount());
    Assert.assertEquals(1L, stats.getZeroCount());
    Assert.assertEquals(2L, stats.getPosCount());
    Assert.assertEquals(1L, stats.getNegCount());
    Assert.assertEquals(-10d, stats.getMin(), ERROR);
    Assert.assertEquals(100d, stats.getMax(), ERROR);
    Assert.assertEquals(25d, stats.getMean(), ERROR);
  }
}
