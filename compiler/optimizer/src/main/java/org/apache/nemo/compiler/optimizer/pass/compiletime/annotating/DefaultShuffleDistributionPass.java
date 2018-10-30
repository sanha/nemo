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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.ShuffleDistributionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.HashMap;

/**
 * Pass for initiating IREdge Metric ExecutionProperty with default key range.
 */
@Requires({CommunicationPatternProperty.class})
@Annotates(ShuffleDistributionProperty.class)
public final class DefaultShuffleDistributionPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DefaultShuffleDistributionPass() {
    super(DefaultShuffleDistributionPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(dst ->
      dag.getIncomingEdgesOf(dst).forEach(edge -> {
        if (CommunicationPatternProperty.Value.Shuffle
            .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())
            && !edge.getPropertyValue(ShuffleDistributionProperty.class).isPresent()) {
          final int parallelism = dst.getPropertyValue(ParallelismProperty.class).get();
          final HashMap<Integer, KeyRange> metric = new HashMap<>();
          for (int i = 0; i < parallelism; i++) { // Consider skew hash partitioner?
            metric.put(i, HashRange.of(i, i + 1, false));
          }
          edge.setProperty(ShuffleDistributionProperty.of(Pair.of(parallelism, metric)));
        }
      }));
    return dag;
  }
}
