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
package edu.snu.nemo.compiler.frontend.spark.core.java;

import org.apache.spark.SparkContext;

import java.util.List;

/**
 * Spark context wrapper for Java.
 */
public final class JavaSparkContext extends org.apache.spark.api.java.JavaSparkContext {
  /**
   * Constructor.
   * @param sparkContext spark context to wrap.
   */
  public JavaSparkContext(final SparkContext sparkContext) {
    super(sparkContext);
  }

  @Override
  public JavaRDD<String> textFile(final String path) {
    return JavaRDD.of(super.sc(), super.textFile(path), path);
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   *
   * @param list input data as list.
   * @param <T> type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  @Override
  public <T> JavaRDD<T> parallelize(final List<T> list) {
    return this.parallelize(list, 1);
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   *
   * @param l input data as list.
   * @param slices number of slices (parallelism).
   * @param <T> type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  @Override
  public <T> JavaRDD<T> parallelize(final List<T> l, final int slices) {
    return JavaRDD.of(super.sc(), l, slices);
  }
}
