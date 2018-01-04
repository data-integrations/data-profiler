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

package co.cask.plugin;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.plugin.statistics.StatisticsOld;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      DataProfiler.class);
  }

  @Test
  public void testStringCaseTransform() throws Exception {
    String inputName = "input";
    String outputName = "output";

    // write the input
    Schema schema = Schema.recordOf(
      "data",
      Schema.Field.of("s", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("i", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("l", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("f", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("d", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

    // create the pipeline config
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputName, schema)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputName)))
      .addStage(new ETLStage("fieldStats", new ETLPlugin(DataProfiler.NAME, BatchAggregator.PLUGIN_TYPE,
                                                         ImmutableMap.<String, String>of())))
      .addConnection("source", "fieldStats")
      .addConnection("fieldStats", "sink")
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));


    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("s", "ab").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", true).build(),
      StructuredRecord.builder(schema)
        .set("s", "xy").set("i", -10).set("l", -10L).set("f", -10f).set("d", -10d).set("b", true).build(),
      StructuredRecord.builder(schema)
        .set("s", "a").set("i", 10).set("l", 10L).set("f", 10f).set("d", 10d).set("b", false).build(),
      StructuredRecord.builder(schema)
        .set("s", "").set("i", 0).set("l", 0L).set("f", 0f).set("d", 0d).set("b", false).build(),
      StructuredRecord.builder(schema).build());

    DataSetManager<Table> inputManager = getDataset(inputName);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 4, TimeUnit.MINUTES);

    // check output
    StatisticsOld stats = new StatisticsOld();
    Set<StructuredRecord> expected = new HashSet<>();
    for (Schema.Field field : schema.getFields()) {
      stats.reset();
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      for (StructuredRecord record : input) {
        stats.update(fieldType, record.get(fieldName));
      }
      expected.add(stats.toRecord(fieldName));
    }

    DataSetManager<Table> outputManager = getDataset(outputName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected.size(), outputRecords.size());
    Assert.assertEquals(expected, new HashSet<>(outputRecords));
  }
}
