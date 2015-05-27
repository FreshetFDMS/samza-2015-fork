/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql.calcite.planner;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Schema;
import org.apache.samza.sql.api.expressions.Expression;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.sql.api.operators.SqlOperatorFactory;
import org.apache.samza.sql.calcite.rel.FilterableStreamScan;
import org.apache.samza.sql.calcite.rel.StreamScan;
import org.apache.samza.sql.calcite.schema.AvroSchemaUtils;
import org.apache.samza.sql.calcite.schema.Stream;
import org.apache.samza.sql.data.avro.AvroSchema;
import org.apache.samza.sql.operators.factory.SimpleOperatorFactoryImpl;
import org.apache.samza.sql.operators.factory.SimpleOperatorSpec;
import org.apache.samza.sql.operators.factory.SimpleRouter;
import org.apache.samza.sql.operators.insert.InsertToStreamSpec;
import org.apache.samza.sql.operators.project.ProjectSpec;
import org.apache.samza.sql.operators.scan.FilterableStreamScanSpec;
import org.apache.samza.sql.operators.scan.StreamScanSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionPlanner extends RelVisitor implements ReflectiveVisitor {
  private static final Logger log = LoggerFactory.getLogger(ExecutionPlanner.class);

  private static final String VISIT_METHOD_NAME = "visit";

  public static final String OP_PROJECTABLE_FILTERABLE_STREAM_SCAN = "projectablefilterablestreamscan";
  public static final String OP_PROJECT = "project";

  private final RelNode logicalPlan;
  private final RelDataTypeFactory relDataTypeFactory;
  private final OperatorRouter router;
  private final RexToJavaCompiler rexToJavaCompiler;
  private final SqlOperatorFactory operatorFactory;
  private final ReflectiveVisitDispatcher<ExecutionPlanner, RelNode> dispatcher =
      ReflectUtil.createDispatcher(ExecutionPlanner.class, RelNode.class);

  private Map<RelNode, List<Object>> relInputs = Maps.newHashMap();
  private OperatorSpec spec;

  private AtomicInteger outputCount = new AtomicInteger(0);

  public ExecutionPlanner(RelNode logicalPlan) {
    this.logicalPlan = logicalPlan;
    this.relDataTypeFactory = logicalPlan.getCluster().getTypeFactory();
    this.rexToJavaCompiler = new RexToJavaCompiler(logicalPlan.getCluster().getRexBuilder());
    this.router = new SimpleRouter();
    this.operatorFactory = new SimpleOperatorFactoryImpl();
  }

  public OperatorRouter getExecutionPlan() {
    // Starting from the root of the logical plan
    visit(logicalPlan, 0, null);
    return router;
  }

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    node.childrenAccept(this);

    dispatcher.invokeVisitor(this, node, VISIT_METHOD_NAME);

    if (parent != null) {
      List<Object> inputs = relInputs.get(parent);
      if (inputs == null) {
        inputs = Lists.newArrayList();
        inputs.addAll(parent.getInputs());
        relInputs.put(parent, inputs);
      }
      inputs.set(ordinal, spec);
    }

    List<Object> inputs = relInputs.get(node);

    SimpleOperator operator = operatorFactory.getOperator(spec);

    if (inputs != null) {
      for (Object inputSpec : inputs) {
        if (inputSpec instanceof StreamScanSpec) {
          /* StreamScan is special because we can ignore it and directly wire the input stream of scan
           * to the stream scan's parent (operator corresponding to current node).
           */
          EntityName input = ((SimpleOperatorSpec) inputSpec).getInputName();

          try {
            // Setting the input to this operator in operators spec.
            // TODO: Figure out a clean way to do this.
            spec.setInputNames(Arrays.asList(input));
            router.addOperator(operator);

            if (log.isDebugEnabled()) {
              log.debug(String.format("Added tuple operator  %s for input %s", spec.getId(), input));
            }
          } catch (Exception e) {
            throw new SamzaException(
                String.format("Unable to add tuple operator. Input: %s, Operator: %s", input, spec.getId()), e);
          }
        } else if (inputSpec instanceof OperatorSpec) {
          OperatorSpec inputOpSpec = (OperatorSpec) inputSpec;
          try {
            spec.setInputNames(inputOpSpec.getOutputNames());
            router.addOperator(operator);

            if (log.isDebugEnabled()) {
              log.debug(String.format("Added tuple operator  %s for inputs %s", spec.getId(), inputOpSpec.getOutputNames()));
            }
          } catch (Exception e) {
            throw new SamzaException(
                String.format("Unable to add tuple operator. Inputs: %s, Operator: %s", inputOpSpec.getOutputNames(), spec.getId()), e);
          }
        } else {
          throw new SamzaException(String.format("Unsupported operator input of type %s", inputSpec.getClass().getCanonicalName()));
        }
      }
    } else {
      if (spec instanceof FilterableStreamScanSpec) {
        EntityName input = ((FilterableStreamScanSpec) spec).getInputName();
        try {
          spec.setInputNames(Arrays.asList(input));
          router.addOperator(operator);
        } catch (Exception e) {
          throw new SamzaException(String.format("Unable to add tuple operator. Input: %s, Operator: %s", input, spec.getId()), e);
        }
      } else if (spec instanceof StreamScanSpec) {
        // Do nothing. We just ignore simple stream scans and directly wire the input stream to next operator.
      } else {
        throw new SamzaException(String.format("Unsupported terminal operator with spec of type: %s", spec.getClass()));
      }
    }
  }

  public void visit(FilterableStreamScan scan) {
    EntityName input = EntityName.getStreamName(Joiner.on(":").join(
        Lists.transform(scan.getTable().getQualifiedName(),
            new Function<String, String>() {
              @Override
              public String apply(String input) {
                return input.toLowerCase();
              }
            })));
    RelOptTable streamTable = scan.getTable();

    Stream stream = streamTable.unwrap(Stream.class);

    if (stream == null) {
      throw new SamzaException("Unknown stream type.");
    }

    Schema streamType = stream.getType(relDataTypeFactory);

    EntityName output = genOutputStreamName(OP_PROJECTABLE_FILTERABLE_STREAM_SCAN);
    Expression filterExpr = rexToJavaCompiler.compile(Lists.newArrayList((RelNode) scan), scan.getFilters());
    this.spec = new FilterableStreamScanSpec(genOperatorId("filterablescan"), input, output, streamType, streamType, filterExpr);
  }

  public void visit(LogicalTableModify modify) {
    if (modify.getOperation() == TableModify.Operation.INSERT) {
      EntityName output = EntityName.getStreamName(Joiner.on(":").join(Lists.transform(modify.getTable().getQualifiedName(),
          new Function<String, String>() {
            @Override
            public String apply(String input) {
              return input.toLowerCase();
            }
          })));
      this.spec = new InsertToStreamSpec(genOperatorId("insert"), null, output);
    } else {
      throw new UnsupportedOperationException(String.format("Stream/table modification of type %s is not supported.", modify.getOperation()));
    }
  }

  public void visit(Project project) {
    RelNode input = project.getInput();
    if (input instanceof StreamScan) {
      EntityName output = genOutputStreamName(OP_PROJECT);
      Expression projectExpr = rexToJavaCompiler.compile(project.getInputs(), project.getProjects());
      Stream inputStream = ((StreamScan) input).getTable().unwrap(Stream.class);

      if (inputStream == null) {
        throw new SamzaException("Input's table should be a Stream.");
      }

      Schema inputStreamType = inputStream.getType(relDataTypeFactory);

      /**
       * Note (Milinda on 04/23/2015):
       * Here we transform type (RelDataType) of project operator to Samza Schema.
       * Because we just use this schema as a metadata to convert incoming message to object array (Object[]), I think
       * type of Schema implementation doesn't matter much. So I am using AvroSchema implementation. But its better
       * if we can implement a dummy Samza Schema to just handle the type information for intermediate operators.
       *
       * TODO: We need to fix this to use proper Schema implementation. This can be a problem otherwise due to incompatible serializers.
       */
      Schema outputSchema = AvroSchema.getSchema(AvroSchemaUtils.relDataTypeToAvroSchema(project.getRowType()));

      this.spec = new ProjectSpec(genOperatorId("project"), null, output, projectExpr, inputStreamType, outputSchema);
    } else {
      // TODO: Remove this once we add other types of scans
      throw new IllegalArgumentException("Unsupported Project input. Only inputs of type StreamScan is supported.");
    }
  }

  public void visit(RelNode relNode) {
    throw new UnsupportedOperationException(String.format("Visiting RelNode of type %s not supported yet.", relNode.getRelTypeName()));
  }

  private EntityName genOutputStreamName(String operator) {
    return EntityName.getStreamName("kafka:".concat(operator).concat("output").concat(Integer.toString(outputCount.getAndIncrement())));
  }

  private String genOperatorId(String operator) {
    // TODO: Implement a proper operator id generator.
    return operator + "-" + UUID.randomUUID();
  }
}
