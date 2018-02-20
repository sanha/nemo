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
package edu.snu.nemo.common.ir.vertex.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;

/**
 * Parallelism ExecutionProperty.
 */
public final class ParallelismProperty extends ExecutionProperty<Integer> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private ParallelismProperty(final Integer value) {
    super(Key.Parallelism, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ParallelismProperty of(final Integer value) {
    return new ParallelismProperty(value);
  }
}
