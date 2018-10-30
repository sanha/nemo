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
package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

import java.util.HashMap;

/**
 * DataCommunicationPattern ExecutionProperty.
 */
public final class OneToOneDistributionProperty
    extends EdgeExecutionProperty<HashMap<Integer, Integer>> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private OneToOneDistributionProperty(final HashMap<Integer, Integer> value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static OneToOneDistributionProperty of(final HashMap<Integer, Integer> value) {
    return new OneToOneDistributionProperty(value);
  }
}
