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
package edu.snu.nemo.compiler.frontend.beam.source;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.beam.sdk.io.BoundedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * SourceVertex implementation for BoundedSource.
 * @param <O> output type.
 */
public final class BeamSampledBoundedSourceVertex<O> extends SourceVertex<O> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamSampledBoundedSourceVertex.class.getName());
  private BoundedSource<O> source;
  private final String sourceDescription;
  private final int numOfOriginalSplit;
  private final int numOfSplitsToSample;

  /**
   * Constructor of BeamBoundedSourceVertex.
   * @param source BoundedSource to read from.
   */
  public BeamSampledBoundedSourceVertex(final BoundedSource<O> source,
                                        final int numOfOriginalSplit,
                                        final int numOfSplitsToSample) {
    this.source = source;
    this.sourceDescription = source.toString();
    this.numOfOriginalSplit = numOfOriginalSplit;
    this.numOfSplitsToSample = numOfSplitsToSample;
  }

  @Override
  public BeamSampledBoundedSourceVertex getClone() {
    final BeamSampledBoundedSourceVertex that =
        new BeamSampledBoundedSourceVertex<>(this.source, numOfSplitsToSample, numOfOriginalSplit);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public BeamSampledBoundedSourceVertex getSampledClone(final int numOfOriginalSplits2,
                                                        final int numOfSplitsToSample2) {
    final BeamSampledBoundedSourceVertex that =
        new BeamSampledBoundedSourceVertex<>(source, numOfOriginalSplits2, numOfSplitsToSample2);
    this.copyExecutionPropertiesTo(that);
    that.setProperty(ParallelismProperty.of(numOfSplitsToSample2));
    return that;
  }

  @Override
  public List<Readable<O>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<O>> readables = new ArrayList<>(numOfSplitsToSample);
    LOG.info("estimate: {}", source.getEstimatedSizeBytes(null));
    LOG.info("original: {}", numOfOriginalSplit);
    LOG.info("desired: {}", desiredNumOfSplits);
    LOG.info("sample to: {}", numOfSplitsToSample);
    final List<? extends BoundedSource<O>> split =
        source.split(source.getEstimatedSizeBytes(null) / numOfOriginalSplit, null);
    if (numOfOriginalSplit != split.size()) {
      throw new RuntimeException("Split number mismatch!");
    }

    final List<Integer> randomIndices = IntStream.range(0, numOfOriginalSplit).boxed().collect(Collectors.toList());
    Collections.shuffle(randomIndices);
    final List<Integer> sampleIndices = randomIndices.subList(0, numOfSplitsToSample);
    for (int i = 0; i < numOfOriginalSplit; i++) {
      if (sampleIndices.contains(i)) {
        readables.add(new BeamBoundedSourceVertex.BoundedSourceReadable<>(split.get(i)));
      }
    }

    return readables;
  }

  @Override
  public void clearInternalStates() {
    source = null;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append(", \"source\": \"");
    sb.append(sourceDescription);
    sb.append("\"}");
    return sb.toString();
  }
}
