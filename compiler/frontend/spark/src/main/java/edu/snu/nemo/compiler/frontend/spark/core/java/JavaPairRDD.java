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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor;
import edu.snu.nemo.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.nemo.compiler.frontend.spark.core.RDD;
import edu.snu.nemo.compiler.frontend.spark.transform.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.Serializer;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.List;
import java.util.Stack;

import static edu.snu.nemo.compiler.frontend.spark.core.java.SparkFrontendUtils.getEdgeCommunicationPattern;

/**
 * Java RDD for pairs.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class JavaPairRDD<K, V> extends org.apache.spark.api.java.JavaPairRDD<K, V> {
  private final SparkContext sparkContext;
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;
  private final IRVertex lastVertex;
  private final Serializer serializer;

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param dag the current DAG.
   * @param lastVertex last vertex added to the builder.
   */
  JavaPairRDD(final SparkContext sparkContext, final DAG<IRVertex, IREdge> dag, final IRVertex lastVertex) {
    // TODO #366: resolve while implementing scala RDD.
    super(RDD.<Tuple2<K, V>>of(sparkContext),
        ClassTag$.MODULE$.apply((Class<K>) Object.class), ClassTag$.MODULE$.apply((Class<V>) Object.class));

    this.loopVertexStack = new Stack<>();
    this.sparkContext = sparkContext;
    this.dag = dag;
    this.lastVertex = lastVertex;
    this.serializer = SparkFrontendUtils.deriveSerializerFrom(sparkContext);
  }

  /**
   * @return the spark context.
   */
  public SparkContext getSparkContext() {
    return sparkContext;
  }

  /////////////// PRIVATE METHODS ///////////////

  /**
   * @return the IR DAG.
   */
  private DAG<IRVertex, IREdge> getDag() {
    return dag;
  }

  /**
   * @return the last vertex.
   */
  private IRVertex getLastVertex() {
    return lastVertex;
  }

  /**
   * @return the serializer.
   */
  private Serializer getSerializer() {
    return serializer;
  }

  /**
   * @return the loop vertex stack.
   */
  private Stack<LoopVertex> getLoopVertexStack() {
    return loopVertexStack;
  }

  /////////////// TRANSFORMATIONS ///////////////

  @Override
  public JavaPairRDD<K, V> reduceByKey(final Function2<V, V, V> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex reduceByKeyVertex = new OperatorVertex(new ReduceByKeyTransform<K, V>(func));
    builder.addVertex(reduceByKeyVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, reduceByKeyVertex),
        lastVertex, reduceByKeyVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaPairRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), reduceByKeyVertex);
  }

  @Override
  public JavaPairRDD<K, Iterable<V>> groupByKey() {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex groupByKeyVertex = new OperatorVertex(new GroupByKeyTransform());
    builder.addVertex(groupByKeyVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, groupByKeyVertex),
        lastVertex, groupByKeyVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaPairRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), groupByKeyVertex);
  }

  @Override
  public <K2, V2> JavaPairRDD<K2, V2> mapToPair(final PairFunction<Tuple2<K, V>, K2, V2> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex mapToPairVertex = new OperatorVertex(new MapToPairTransform<>(func));
    builder.addVertex(mapToPairVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapToPairVertex),
        lastVertex, mapToPairVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaPairRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), mapToPairVertex);
  }

  @Override
  public <R> JavaRDD<R> map(final Function<Tuple2<K, V>, R> f) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex mapVertex = new OperatorVertex(new MapTransform<>(f));
    builder.addVertex(mapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapVertex),
        lastVertex, mapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), mapVertex);
  }

  /**
   * PairRDD1(K, V) -> (oneToOne) -> PairRDD1(K, Union(V, W)) -> (shuffle) -> PairRDD(K, Tuple2(Itr(V), Itr(W)).
   * PairRDD2(K, W) -> (oneToOne) -> PairRDD2(K, Union(V, W)) -> (shuffle)
   *
   * @param other the other pair RDD to cogroup.
   * @param <W>   the type of the value of the other RDD.
   * @return the co-grouped RDD.
   */
  @Override
  public <W> JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(
      final org.apache.spark.api.java.JavaPairRDD<K, W> other) {
    if (other instanceof JavaPairRDD) {
      final JavaPairRDD otherPairRdd = (JavaPairRDD) other;

      // Map to union RDDs.
      final JavaPairRDD<K, CoGroup.Union> rdd1 = this.mapToPair(CoGroup.<K, V>mapFunction(0));
      final JavaPairRDD<K, CoGroup.Union> rdd2 = otherPairRdd.mapToPair(CoGroup.<K, W>mapFunction(1));

      // Construct common DAG builder.
      final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(rdd1.getDag());
      final DAG<IRVertex, IREdge> otherDag = rdd2.getDag();
      otherDag.getVertices().forEach(v -> builder.addVertex(v, otherDag));
      otherDag.getVertices().forEach(v -> otherDag.getIncomingEdgesOf(v).forEach(builder::connectVertices));

      // Add a new co-group vertex.
      final IRVertex coGroupVertex = new OperatorVertex(CoGroup.<K, V, W>coGroupTransform());
      builder.addVertex(coGroupVertex, rdd1.getLoopVertexStack());

      final IREdge newEdge1 = new IREdge(getEdgeCommunicationPattern(rdd1.getLastVertex(), coGroupVertex),
          rdd1.getLastVertex(), coGroupVertex, new SparkCoder(rdd1.getSerializer()));
      newEdge1.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
      builder.connectVertices(newEdge1);

      final IREdge newEdge2 = new IREdge(getEdgeCommunicationPattern(rdd2.getLastVertex(), coGroupVertex),
          rdd2.getLastVertex(), coGroupVertex, new SparkCoder(rdd2.getSerializer()));
      newEdge2.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
      builder.connectVertices(newEdge2);

      return new JavaPairRDD<>(rdd1.getSparkContext(), builder.buildWithoutSourceSinkCheck(), coGroupVertex);
    } else {
      throw new UnsupportedOperationException("Cannot cogroup with Nemo RDD and Spark RDD!");
    }
  }

  /////////////// ACTIONS ///////////////

  @Override
  public List<Tuple2<K, V>> collect() {
    return SparkFrontendUtils.collect(dag, loopVertexStack, lastVertex, serializer);
  }

  //TODO#776: support unimplemented RDD transformation/actions.
}
