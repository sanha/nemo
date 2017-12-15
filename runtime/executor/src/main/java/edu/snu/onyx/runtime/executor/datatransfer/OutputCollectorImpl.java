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
package edu.snu.onyx.runtime.executor.datatransfer;

import edu.snu.onyx.common.ir.OutputCollector;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Output Collector Implementation.
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private AtomicReference<LinkedBlockingQueue<O>> outputQueue;

  /**
   * Constructor of a new OutputCollector.
   */
  public OutputCollectorImpl() {
    outputQueue = new AtomicReference<>(new LinkedBlockingQueue<>());
  }

  @Override
  public void emit(final O output) {
    try {
      outputQueue.get().put(output);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while OutputCollectorImpl#emit", e);
    }
  }

  @Override
  public void emit(final String dstVertexId, final Object output) {
    throw new UnsupportedOperationException("emit(dstVertexId, output) in OutputCollectorImpl.");
  }

  /**
   * Inter-Task data is transferred from sender-side Task's OutputCollectorImpl to receiver-side Task.
   * @return the output element that is transferred to the next Task of TaskGroup.
   */
  public O remove() {
    return outputQueue.get().remove();
  }

  public boolean isEmpty() {
    return outputQueue.get().isEmpty();
  }

  /**
   * Collects the accumulated output and replace the output list.
   *
   * @return the list of output elements.
   */
  public List<O> collectOutputList() {
    LinkedBlockingQueue<O> currentOutputQueue = outputQueue.getAndSet(new LinkedBlockingQueue<>());
    List<O> outputList = new ArrayList<>();
    while (currentOutputQueue.size() > 0) {
      outputList.add(currentOutputQueue.remove());
    }
    return outputList;
  }
}
