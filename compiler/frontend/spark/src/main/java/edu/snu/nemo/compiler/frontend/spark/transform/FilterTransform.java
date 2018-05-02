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
package edu.snu.nemo.compiler.frontend.spark.transform;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.Function;

/**
 * Filter transform for Spark.
 *
 * @param <I> input type.
 */
public final class FilterTransform<I> implements Transform<I, I> {
  private final Function<I, Boolean> filterFunc;
  private OutputCollector<I> outputCollector;

  /**
   * Constructor.
   * @param filterFunc the function to run filter with.
   */
  public FilterTransform(final Function<I, Boolean> filterFunc) {
    this.filterFunc = filterFunc;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<I> p) {
    this.outputCollector = p;
  }

  public void onData(final I element) {
    try {
      if (filterFunc.call(element)) {
        outputCollector.emit(element);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
