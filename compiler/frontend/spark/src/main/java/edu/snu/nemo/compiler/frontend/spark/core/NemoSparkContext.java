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
package edu.snu.nemo.compiler.frontend.spark.core;

import edu.snu.nemo.compiler.frontend.spark.core.rdd.RDD;
import org.apache.spark.SparkContext;

import java.util.List;

/**
 * Spark context wrapper for in Nemo.
 */
public final class NemoSparkContext extends org.apache.spark.SparkContext {
  private final org.apache.spark.SparkContext sparkContext;

  /**
   * Constructor.
   *
   * @param sparkContext spark context to wrap.
   */
  public NemoSparkContext(final SparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   *
   * @param l      input data as list.
   * @param slices number of slices (parallelism).
   * @param <T>    type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  public <T> RDD<T> parallelize(final List<T> l, final int slices) {
    return RDD.of(this.sparkContext, l, slices);
  }
}
