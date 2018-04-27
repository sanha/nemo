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
import org.apache.hadoop.mapred.InputSplit;
import org.apache.spark.Partition;
import org.apache.spark.SerializableWritable;
import org.apache.spark.TaskContext$;
import org.apache.spark.rdd.HadoopPartition;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Bounded source vertex for Spark.
 */
public final class SparkTextFileSourceVertex extends SourceVertex<String> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkTextFileSourceVertex.class.getName());
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
    for (int i = 0; i < numPartitions; i++) {
      readables.add(new SparkBoundedSourceReadable(sparkSession, sparkSession.getInitialConf(), i));
    }
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
    private final List<String> locations;

    /**
     * Constructor.
     *
     * @param sessionInitialConf spark session's initial configuration.
     * @param partitionIndex     partition for this readable.
     */
    private SparkBoundedSourceReadable(final SparkSession sparkSession,
                                       final Map<String, String> sessionInitialConf,
                                       final int partitionIndex) {
      this.sessionInitialConf = sessionInitialConf;
      this.partitionIndex = partitionIndex;
      final RDD<String> rdd = SparkSession.initializeTextFileRDD(sparkSession, inputPath, numPartitions);
      final Partition partition = rdd.getPartitions()[partitionIndex];

      try {
        if (partition instanceof HadoopPartition) {
          final Field inputSplitField = partition.getClass().getDeclaredField("inputSplit");
          inputSplitField.setAccessible(true);
          final InputSplit inputSplit = (InputSplit) ((SerializableWritable) inputSplitField.get(partition)).value();

          //final String[] splitLocations = inputSplit.getLocations();
          final String[] splitLocations = new String[0];

          final StringBuilder sb = new StringBuilder("(");
          for (final String loc : splitLocations) {
            sb.append(loc);
            sb.append(", ");
          }
          sb.append(")");
          LOG.info(sb.toString());

          locations = Arrays.asList(splitLocations);
        } else {
          locations = Collections.emptyList();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Iterable<String> read() {
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
      if (locations.isEmpty()) {
        throw new UnsupportedOperationException();
      } else {
        return locations;
      }
    }
  }
}
