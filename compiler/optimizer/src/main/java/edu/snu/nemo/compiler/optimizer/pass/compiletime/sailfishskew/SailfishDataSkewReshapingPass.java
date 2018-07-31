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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.EncoderProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.transform.DummyTransform;
import edu.snu.nemo.common.ir.vertex.transform.RelayTransform;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping.ReshapingPass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pass to modify the DAG for a job to perform data skew.
 * It adds a {@link MetricCollectionBarrierVertex} before Shuffle edges, to make a barrier before it,
 * and to use the metrics to repartition the skewed data.
 * NOTE: we currently put the DataSkewCompositePass at the end of the list for each policies, as it needs to take
 * a snapshot at the end of the pass. This could be prevented by modifying other passes to take the snapshot of the
 * DAG at the end of each passes for metricCollectionVertices.
 */
public final class SailfishDataSkewReshapingPass extends ReshapingPass {
  /**
   * Default constructor.
   */
  public SailfishDataSkewReshapingPass() {
    super(Collections.singleton(ParallelismProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final List<MetricCollectionBarrierVertex> metricCollectionVertices = new ArrayList<>();

    dag.topologicalDo(v -> {
      // Maintain original DAG as is.
      builder.addVertex(v);
      dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);

      // We care about OperatorVertices that have any incoming edges that are of type Shuffle.
      if (v instanceof OperatorVertex && ((OperatorVertex) v).getTransform() instanceof RelayTransform) {
        final IREdge edgeToRelay = dag.getIncomingEdgesOf(v).get(0);
        final IRVertex predRelayVtx = edgeToRelay.getSrc();
        final Pair<IRVertex, IRVertex> vtxPair = addSamplingDag(dag, builder, predRelayVtx);
        final IRVertex originalDagSourceVtx = vtxPair.left();
        final IRVertex sampledDagLastVtx = vtxPair.right();

        final MetricCollectionBarrierVertex<Integer, Long> metricCollectionBarrierVertex
            = new MetricCollectionBarrierVertex<>();
        metricCollectionBarrierVertex.setProperty(ParallelismProperty.of(
            sampledDagLastVtx.getPropertyValue(ParallelismProperty.class).get()));
        metricCollectionVertices.add(metricCollectionBarrierVertex);
        builder.addVertex(metricCollectionBarrierVertex);

        // We then insert the dynamicOptimizationVertex between the vertex and incoming vertices.
        final IREdge newEdgeToMC = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
            sampledDagLastVtx, metricCollectionBarrierVertex);
        newEdgeToMC.setProperty(EncoderProperty.of(edgeToRelay.getPropertyValue(EncoderProperty.class).get()));
        newEdgeToMC.setProperty(DecoderProperty.of(edgeToRelay.getPropertyValue(DecoderProperty.class).get()));
        builder.connectVertices(newEdgeToMC);

        final OperatorVertex dummyVtx = new OperatorVertex(new DummyTransform());
        dummyVtx.setProperty(ParallelismProperty.of(v.getPropertyValue(ParallelismProperty.class).get()));
        builder.addVertex(dummyVtx);

        final IREdge newEdgeToDummy = new IREdge(DataCommunicationPatternProperty.Value.Shuffle,
            metricCollectionBarrierVertex, dummyVtx);
        edgeToRelay.copyExecutionPropertiesTo(newEdgeToDummy);
        newEdgeToDummy.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
        builder.connectVertices(newEdgeToDummy);

        final IREdge newEdgeFromDummy = new IREdge(DataCommunicationPatternProperty.Value.BroadCast, // no output
            dummyVtx, originalDagSourceVtx);
        newEdgeFromDummy.setProperty(EncoderProperty.of(edgeToRelay.getPropertyValue(EncoderProperty.class).get()));
        newEdgeFromDummy.setProperty(DecoderProperty.of(edgeToRelay.getPropertyValue(DecoderProperty.class).get()));
        builder.connectVertices(newEdgeFromDummy);
      } //else { // Others are simply added to the builder, unless it comes from an updated vertex
    });
    final DAG<IRVertex, IREdge> newDAG = builder.build();
    metricCollectionVertices.forEach(v -> v.setDAGSnapshot(newDAG));
    return newDAG;
  }

  /**
   * @return the pair of first vertex of the original dag and last vertex of cloned (sampled) dag.
   */
  private Pair<IRVertex, IRVertex> addSamplingDag(final DAG<IRVertex, IREdge> dagToSample,
                                                  final DAGBuilder<IRVertex, IREdge> builder,
                                                  final IRVertex lastVtx) {
    final IRVertex clonedVtx;
    if (lastVtx instanceof SourceVertex) {
      clonedVtx = ((SourceVertex) lastVtx)
          .getSampledClone((int) Math.ceil((double) lastVtx.getPropertyValue(ParallelismProperty.class)
              .orElseThrow(() -> new RuntimeException("no parallelism!")) / 100));
      builder.addVertex(clonedVtx);
      return Pair.of(lastVtx, clonedVtx);
    } else {
      clonedVtx = lastVtx.getClone();
      builder.addVertex(clonedVtx);
      clonedVtx.setProperty(ParallelismProperty.of(lastVtx.getPropertyValue(ParallelismProperty.class)
          .orElseThrow(() -> new RuntimeException("no parallelism!")) / 100));

      // WARNING: assume single source vtx. let's refactor this to recognize stage.
      IRVertex originSourceVtx = lastVtx;
      for (final IREdge edge : dagToSample.getIncomingEdgesOf(lastVtx)) {
        final Pair<IRVertex, IRVertex> pair = addSamplingDag(dagToSample, builder, edge.getSrc());
        originSourceVtx = pair.left();
        final IRVertex predClonedVtx = pair.right();
        final IREdge clonedEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class)
            .orElseThrow(() -> new RuntimeException("No communciation!")),
            predClonedVtx, clonedVtx, edge.isSideInput());
        edge.copyExecutionPropertiesTo(clonedEdge);
        builder.connectVertices(clonedEdge);
      }
      return Pair.of(originSourceVtx, clonedVtx);
    }
  }
}
