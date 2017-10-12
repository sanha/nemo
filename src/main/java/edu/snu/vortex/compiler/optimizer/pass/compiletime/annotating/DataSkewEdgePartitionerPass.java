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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.PartitionerProperty;
import edu.snu.vortex.compiler.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.DataSkewHashPartitioner;

import java.util.Collections;
import java.util.List;

/**
 * Pado pass for tagging edges with {@link PartitionerProperty}.
 */
public final class DataSkewEdgePartitionerPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DataSkewEdgePartitionerPass";

  public DataSkewEdgePartitionerPass() {
    super(ExecutionProperty.Key.Partitioner, Collections.singleton(ExecutionProperty.Key.DataCommunicationPattern));
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      if (vertex instanceof MetricCollectionBarrierVertex) {
        final List<IREdge> outEdges = dag.getOutgoingEdgesOf(vertex);
        outEdges.forEach(edge -> {
          // double checking.
          if (DataSkewRuntimePass.class.equals(edge.getProperty(ExecutionProperty.Key.MetricCollection))) {
            edge.setProperty(PartitionerProperty.of(DataSkewHashPartitioner.class));
          }
        });
      }
    });
    return dag;
  }
}