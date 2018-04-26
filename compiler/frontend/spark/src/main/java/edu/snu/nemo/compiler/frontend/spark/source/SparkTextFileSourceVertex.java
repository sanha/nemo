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
package edu.snu.nemo.compiler.frontend.spark.source;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import org.apache.spark.TaskContext$;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Bounded source vertex for Spark.
 */
public final class SparkTextFileSourceVertex extends SourceVertex<String> {
  private final List<Readable<String>> readables;
  private final String inputPath;
  private final int numPartitions;

  /**
   * Constructor.
   *
   * @param sparkSession  sparkSession to recreate on each executor.
   * @param inputPath     the input text file path.
   * @param numPartitions the number of partitions.
   */
  public SparkTextFileSourceVertex(final SparkSession sparkSession,
                                   final String inputPath,
                                   final int numPartitions) {
    this.readables = new ArrayList<>();
    this.inputPath = inputPath;
    this.numPartitions = numPartitions;
    IntStream.range(0, numPartitions).forEach(partitionIndex ->
        readables.add(new SparkBoundedSourceReadable(
            sparkSession.getInitialConf(),
            partitionIndex)));
  }

  /**
   * Constructor for clone.
   *
   * @param readables     the list of Readables to set.
   * @param inputPath     the input text file path.
   * @param numPartitions the number of partitions.
   */
  public SparkTextFileSourceVertex(final List<Readable<String>> readables,
                                   final String inputPath,
                                   final int numPartitions) {
    this.readables = readables;
    this.inputPath = inputPath;
    this.numPartitions = numPartitions;
  }

  @Override
  public SparkTextFileSourceVertex getClone() {
    final SparkTextFileSourceVertex that = new SparkTextFileSourceVertex(this.readables, inputPath, numPartitions);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Readable<String>> getReadables(final int desiredNumOfSplits) {
    return readables;
  }

  /**
   * A Readable for SparkBoundedSourceReadablesWrapper.
   */
  private final class SparkBoundedSourceReadable implements Readable<String> {
    private final Map<String, String> sessionInitialConf;
    private final int partitionIndex;

    /**
     * Constructor.
     *
     * @param sessionInitialConf spark session's initial configuration.
     * @param partitionIndex     partition for this readable.
     */
    private SparkBoundedSourceReadable(final Map<String, String> sessionInitialConf,
                                       final int partitionIndex) {
      this.sessionInitialConf = sessionInitialConf;
      this.partitionIndex = partitionIndex;
    }

    @Override
    public Iterable<String> read() throws Exception {
      // for setting up the same environment in the executors.
      final SparkSession spark = SparkSession.builder()
          .config(sessionInitialConf)
          .getOrCreate();
      final RDD<String> rdd = SparkSession.initializeTextFileRDD(spark, inputPath, numPartitions);

      // Spark does lazy evaluation: it doesn't load the full dataset, but only the partition it is asked for.
      return () -> JavaConverters.asJavaIteratorConverter(
          rdd.iterator(rdd.getPartitions()[partitionIndex], TaskContext$.MODULE$.empty())).asJava();
    }

    @Override
    public List<String> getLocations() {
      /*
      final SparkSession spark = SparkSession.builder()
          .config(sessionInitialConf)
          .getOrCreate();
      final RDD<String> rdd = SparkSession.initializeTextFileRDD(spark, inputPath, numPartitions);
      final Seq<String> locationSeq = rdd.getPreferredLocations(rdd.getPartitions()[partitionIndex]);

      final List<String> locationList = new ArrayList<>(locationSeq.size());
      final Iterator<String> itr = locationSeq.iterator();
      while (itr.hasNext()) {
        locationList.add(itr.next());
      }

      if (locationList.isEmpty()) {
        throw new UnsupportedOperationException();
      } else {
        return locationList;
      }*/
      throw new UnsupportedOperationException();
    }
  }
}
