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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.HashMap;

/**
 * A pass for annotate duplicate data for each edge.
 */
public final class DuplicateEdgeGroupSizePass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public DuplicateEdgeGroupSizePass() {
    super(ExecutionProperty.Key.DuplicateEdgeGroup);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final HashMap<String, Integer> groupIdToGroupSize = new HashMap<>();
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final DuplicateEdgeGroupPropertyValue duplicateEdgeGroupProperty =
              e.getProperty(ExecutionProperty.Key.DuplicateEdgeGroup);
          if (duplicateEdgeGroupProperty != null) {
            final String groupId = duplicateEdgeGroupProperty.getGroupId();
            final Integer currentCount = groupIdToGroupSize.getOrDefault(groupId, 0);
            groupIdToGroupSize.put(groupId, currentCount + 1);
          }
        }));

    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final DuplicateEdgeGroupPropertyValue duplicateEdgeGroupProperty =
              e.getProperty(ExecutionProperty.Key.DuplicateEdgeGroup);
          if (duplicateEdgeGroupProperty != null) {
            final String groupId = duplicateEdgeGroupProperty.getGroupId();
            if (groupIdToGroupSize.containsKey(groupId)) {
              duplicateEdgeGroupProperty.setGroupSize(groupIdToGroupSize.get(groupId));
            }
          }
        }));

    return dag;
  }
}
