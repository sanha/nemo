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
package edu.snu.nemo.compiler.frontend.spark.core.rdd;

import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor;
import edu.snu.nemo.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils;
import edu.snu.nemo.compiler.frontend.spark.transform.ReduceByKeyTransform;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.List;

/**
 * Java RDD for pairs.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class JavaPairRDD<K, V> extends org.apache.spark.api.java.JavaPairRDD<K, V> {
  private final RDD<Tuple2<K, V>> rdd;

  /**
   * Constructor.
   *
   * @param rdd the rdd to wrap.
   */
  private JavaPairRDD(final RDD<Tuple2<K, V>> rdd) {
    super(rdd, ClassTag$.MODULE$.apply((Class<K>) Object.class), ClassTag$.MODULE$.apply((Class<V>) Object.class));

    this.rdd = rdd;
  }

  /////////////// TRANSFORMATIONS ///////////////

  @Override
  public JavaPairRDD<K, V> reduceByKey(final Function2<V, V, V> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex reduceByKeyVertex = new OperatorVertex(new ReduceByKeyTransform<K, V>(func));
    builder.addVertex(reduceByKeyVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, reduceByKeyVertex),
        lastVertex, reduceByKeyVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaPairRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), reduceByKeyVertex);
  }

  /////////////// ACTIONS ///////////////

  @Override
  public List<Tuple2<K, V>> collect() {
    return rdd.collectToList();
  }

  //TODO#776: support unimplemented RDD transformation/actions.
}
