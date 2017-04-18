/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test Vortex Backend.
 */
public final class VortexBackendTest<I, O> {
  private final IRVertex source = new BoundedSourceVertex<>(new TestUtil.EmptyBoundedSource("Source"));
  private final IRVertex map1 = new OperatorVertex(new TestUtil.EmptyTransform("MapElements"));
  private final IRVertex groupByKey = new OperatorVertex(new TestUtil.EmptyTransform("GroupByKey"));
  private final IRVertex combine = new OperatorVertex(new TestUtil.EmptyTransform("Combine"));
  private final IRVertex map2 = new OperatorVertex(new TestUtil.EmptyTransform("MapElements"));

  private final DAGBuilder builder = new DAGBuilder<IRVertex, IREdge>();
  private DAG<IRVertex, IREdge> dag;

  @Before
  public void setUp() throws Exception {
    builder.addVertex(source);
    builder.addVertex(map1);
    builder.addVertex(groupByKey);
    builder.addVertex(combine);
    builder.addVertex(map2);
    builder.connectVertices(new IREdge(IREdge.Type.OneToOne, source, map1));
    builder.connectVertices(new IREdge(IREdge.Type.ScatterGather, map1, groupByKey));
    builder.connectVertices(new IREdge(IREdge.Type.OneToOne, groupByKey, combine));
    builder.connectVertices(new IREdge(IREdge.Type.OneToOne, combine, map2));


    this.dag = builder.build();

    System.out.println(dag);

    this.dag = new Optimizer().optimize(dag, Optimizer.PolicyType.Pado);
  }

  /**
   * This method uses an IR DAG and tests whether VortexBackend successfully generates an Execution Plan.
   * @throws Exception during the Execution Plan generation.
   */
  @Test
  public void testExecutionPlanGeneration() throws Exception {
    final Backend<ExecutionPlan> backend = new VortexBackend();
    final ExecutionPlan executionPlan = backend.compile(dag);

    assertEquals(executionPlan.getRuntimeStageDAG().getVertices().size(),  2);
    assertEquals(executionPlan.getRuntimeStageDAG().getTopologicalSort().get(0)
        .getStageInternalDAG().getVertices().size(), 2);
    assertEquals(executionPlan.getRuntimeStageDAG().getTopologicalSort()
        .get(1).getStageInternalDAG().getVertices().size(), 3);
  }
}