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
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Schema;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.SqlOperatorFactory;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.spec.OperatorSpec;
import org.apache.samza.sql.api.router.OperatorRouter;
import org.apache.samza.sql.api.expressions.Expression;
import org.apache.samza.sql.calcite.schema.RelDataTypeUtils;
import org.apache.samza.sql.calcite.schema.Stream;
import org.apache.samza.sql.operators.factory.SimpleOperatorFactoryImpl;
import org.apache.samza.sql.operators.insert.InsertToStreamSpec;
import org.apache.samza.sql.operators.scan.ProjectableFilterableStreamScanSpec;
import org.apache.samza.sql.operators.scan.StreamScanSpec;
import org.apache.samza.sql.calcite.rel.ProjectableFilterableStreamScan;
import org.apache.samza.sql.router.SimpleRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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

    Operator operator = spec.getType() == OperatorSpec.Type.TUPLE ?
        operatorFactory.getTupleOperator(spec) : operatorFactory.getRelationOperator(spec);

    if (inputs != null) {
      for (int i = 0; i < inputs.size(); i++) {
        Object inputSpec = inputs.get(i);
        if (inputSpec instanceof StreamScanSpec) {
        /* StreamScan is special because we can ignore it and directly wire the input stream of scan
         * to the stream scan's parent (operator corresponding to current node).
         */
          EntityName input = ((StreamScanSpec) inputSpec).getOutputName();
          try {
            router.addTupleOperator(input, (TupleOperator) operator);

            if (log.isDebugEnabled()) {
              log.debug(String.format("Added tuple operator  %s for input %s", spec.getId(), input));
            }
          } catch (Exception e) {
            throw new SamzaException(
                String.format("Unable to add tuple operator. Input: %s, Operator: %s", input, spec.getId()), e);
          }
        } else if (inputSpec instanceof OperatorSpec) {
          OperatorSpec inputOpSpec = (OperatorSpec) inputSpec;
          if (spec.getType() == OperatorSpec.Type.TUPLE) {
            for (EntityName output : inputOpSpec.getOutputNames()) {
              try {
                router.addTupleOperator(output, (TupleOperator) operator);

                if (log.isDebugEnabled()) {
                  log.debug(String.format("Added tuple operator  %s for input %s", spec.getId(), output));
                }
              } catch (Exception e) {
                throw new SamzaException(
                    String.format("Unable to add tuple operator. Input: %s, Operator: %s", output, spec.getId()), e);
              }
            }
          } else {
            for (EntityName output : inputOpSpec.getOutputNames()) {
              try {
                router.addRelationOperator(output, (RelationOperator) operator);

                if (log.isDebugEnabled()) {
                  log.debug(String.format("Added relation operator  %s for input %s", spec.getId(), output));
                }
              } catch (Exception e) {
                throw new SamzaException(
                    String.format("Unable to add relation operator. Input: %s, Operator: %s", output, spec.getId()), e);
              }
            }
          }
        } else {
          throw new SamzaException(String.format("Unsupported operator input of type %s", inputSpec.getClass().getCanonicalName()));
        }
      }
    } else {
      if (spec instanceof ProjectableFilterableStreamScanSpec) {
        EntityName input = ((ProjectableFilterableStreamScanSpec) spec).getInputName();
        try {
          router.addTupleOperator(input, (TupleOperator) operator);
        } catch (Exception e) {
          throw new SamzaException(String.format("Unable to add tuple operator. Input: %s, Operator: %s", input, spec.getId()), e);
        }
      } else {
        throw new SamzaException(String.format("Unsupported terminal operator of type: %s", spec.getType()));
      }
    }
  }

  public void visit(ProjectableFilterableStreamScan scan) {
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

    if(stream == null){
      throw new SamzaException("Unknown stream type.");
    }

    Schema streamType = stream.getType(relDataTypeFactory);

    EntityName output = genOutputStreamName(OP_PROJECTABLE_FILTERABLE_STREAM_SCAN);
    Expression filterExpr = rexToJavaCompiler.compile(Lists.newArrayList((RelNode) scan), scan.getFilters());
    this.spec = new ProjectableFilterableStreamScanSpec(input, output, streamType, streamType, filterExpr);
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
      this.spec = new InsertToStreamSpec(null, output);
    } else {
      throw new UnsupportedOperationException(String.format("Stream/table modification of type %s is not supported.", modify.getOperation()));
    }
  }

  public void visit(Project project) {
//    EntityName output = genOutputStreamName(OP_PROJECT);
//    Expression projectExpr = rexToJavaCompiler.compile(project.getInputs(), project.getProjects());
//    this.spec = new ProjectSpec(null, output, projectExpr, project.getInput().getRowType(), project.getRowType());
    throw new UnsupportedOperationException("Project operator is not supported yet.");
  }

  public void visit(RelNode relNode) {
    throw new UnsupportedOperationException(String.format("Visiting RelNode of type %s not supported yet.", relNode.getRelTypeName()));
  }

  private EntityName genOutputStreamName(String operator) {
    return EntityName.getStreamName("kafka:".concat(operator).concat("output").concat(Integer.toString(outputCount.getAndIncrement())));
  }
}
