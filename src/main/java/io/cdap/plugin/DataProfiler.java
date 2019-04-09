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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.plugin.profiles.*;

import javax.annotation.Nullable;
import javax.ws.rs.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Aggregator that calculates statistics for each record field.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name(DataProfiler.NAME)
@Description("Calculates statistics for each input record field. For every field, a total count and null count will" +
  "be calculated. For numeric fields, min, max, average, stddev, zero count, positive count, and negative count " +
  "will be calculated. For string fields, min length, max length, avg length, and empty count will be calculated. " +
  "For boolean fields, true and false counts will be calculated.")
public class DataProfiler extends BatchAggregator<String, StructuredRecord, StructuredRecord> {
  public static final String NAME = "DataProfiler";
  private final Conf conf;
  private Profiler executor;
  private Schema schema;
  private static final List<Profile> profiles = Arrays.asList(
    new Categorical(),
    new Logical(),
    new Quantitative(),
    new Uniques(),
    new Histogram()
  );

  public DataProfiler(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    conf.validate();
    executor = new DefaultProfiler(profiles,
                                   configurer.getStageConfigurer().getInputSchema()
    );
    configurer.getStageConfigurer().setOutputSchema(executor.getOutputSchema());
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    if (conf.numPartitions == null) {
      Schema inputSchema = context.getInputSchema();
      if (inputSchema != null) {
        context.setNumPartitions(inputSchema.getFields().size());
      }
    } else {
      context.setNumPartitions(conf.numPartitions);
    }
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchAggregatorContext context) {
    super.onRunFinish(succeeded, context);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    executor = new DefaultProfiler(profiles, context.getInputSchema());
  }

  @Override
  public void groupBy(StructuredRecord input, Emitter<String> groupKeyEmitter)
    throws Exception {
    for (Schema.Field field : input.getSchema().getFields()) {
      groupKeyEmitter.emit(field.getName());
    }
  }

  @Override
  public void aggregate(String name, Iterator<StructuredRecord> values,
                        Emitter<StructuredRecord> emitter) throws Exception {
    executor.reset();
    while (values.hasNext()) {
      StructuredRecord record = values.next();
      Schema.Field field = record.getSchema().getField(name);
      if (field == null || !field.getSchema().isSimpleOrNullableSimple()) {
        continue;
      }
      executor.update(name, record.get(name));
    }
    StructuredRecord result = executor.result(name);
    emitter.emit(result);
  }

  class Request {}

  /**
   * This method retrieves the schema of the profiler output schema.
   *
   * @param request empty object.
   * @return Translated schema.
   * @throws Exception
   */
  @Path("getSchema")
  public Schema getSchema(Request request) throws Exception {
    return new DefaultProfiler(profiles, null).getOutputSchema();
  }


  public static class Conf extends PluginConfig {
    @Nullable
    @Description("The number of partitions to use when shuffling the data. " +
      "Defaults to the number of fields in the input fields.")
    @Name("partitions")
    private Integer numPartitions;

    private void validate() {
      if (numPartitions != null && numPartitions < 1) {
        throw new IllegalArgumentException("Invalid number of partitions: " + numPartitions + ". Must be at least 1.");
      }
    }
  }
}
