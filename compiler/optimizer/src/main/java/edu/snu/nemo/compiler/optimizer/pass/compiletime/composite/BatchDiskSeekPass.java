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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.BatchDiskSeekEdgeDataFlowModelPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.BatchDiskSeekEdgeDataStorePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.BatchDiskSeekEdgeUsedDataHandlingPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping.BatchDiskSeekRelayReshapingPass;

import java.util.Arrays;

/**
 * A series of passes to support disk seek batching during shuffle read.
 */
public final class BatchDiskSeekPass extends CompositePass {
  /**
   * Default constructor.
   */
  public BatchDiskSeekPass() {
    super(Arrays.asList(
        new BatchDiskSeekRelayReshapingPass(),
        new BatchDiskSeekEdgeDataFlowModelPass(),
        new BatchDiskSeekEdgeDataStorePass(),
        new BatchDiskSeekEdgeUsedDataHandlingPass()
    ));
  }
}
