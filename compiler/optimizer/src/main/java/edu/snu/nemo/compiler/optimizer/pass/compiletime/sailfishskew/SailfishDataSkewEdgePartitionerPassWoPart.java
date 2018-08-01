/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.sailfishskew;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

import java.util.Collections;
import java.util.List;

/**
 * Pado pass for tagging edges with {@link PartitionerProperty}.
 */
public final class SailfishDataSkewEdgePartitionerPassWoPart extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SailfishDataSkewEdgePartitionerPassWoPart() {
    super(PartitionerProperty.class, Collections.singleton(MetricCollectionProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      if (dag.getOutgoingEdgesOf(vertex).stream()
          .anyMatch(e -> e.getPropertyValue(DataCommunicationPatternProperty.class)
              .orElseThrow(() -> new RuntimeException("no comm!"))
              .equals(DataCommunicationPatternProperty.Value.Shuffle))) {
        final List<IREdge> outEdges = dag.getOutgoingEdgesOf(vertex);
        outEdges.forEach(edge -> {
          // double checking.
          if (edge.getPropertyValue(DataCommunicationPatternProperty.class)
              .orElseThrow(() -> new RuntimeException("no comm!"))
              .equals(DataCommunicationPatternProperty.Value.Shuffle)) {
            edge.setProperty(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));
          }
        });
      }
    });
    return dag;
  }
}