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
package edu.snu.coral.runtime.executor.data.stores;

import edu.snu.coral.common.coder.Coder;
import edu.snu.coral.runtime.executor.data.CoderManager;
import edu.snu.coral.runtime.executor.data.block.NonSerializedMemoryBlock;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * Store data in local memory.
 */
@ThreadSafe
public final class MemoryStore extends LocalBlockStore {

  /**
   * Constructor.
   *
   * @param coderManager the coder manager.
   */
  @Inject
  private MemoryStore(final CoderManager coderManager) {
    super(coderManager);
  }

  /**
   * @see BlockStore#createBlock(String)
   */
  @Override
  public void createBlock(final String blockId) {
    final Coder coder = getCoderFromWorker(blockId);
    getBlockMap().put(blockId, new NonSerializedMemoryBlock(coder));
  }

  /**
   * @see BlockStore#removeBlock(String)
   */
  @Override
  public Boolean removeBlock(final String blockId) {
    return getBlockMap().remove(blockId) != null;
  }
}