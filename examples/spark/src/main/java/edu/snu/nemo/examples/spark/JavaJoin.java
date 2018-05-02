/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.examples.spark;

import edu.snu.nemo.compiler.frontend.spark.core.java.JavaPairRDD;
import edu.snu.nemo.compiler.frontend.spark.core.java.JavaRDD;
import edu.snu.nemo.compiler.frontend.spark.core.java.JavaSparkContext;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Computes an approximation to pi.
 * Usage: JavaSparkPi [partitions]
 */
public final class JavaJoin {

  /**
   * Private constructor.
   */
  private JavaJoin() {
  }

  /**
   * Main method.
   *
   * @param args arguments.
   * @throws Exception exceptions.
   */
  public static void main(final String[] args) throws Exception {

    final String input0FilePath = args[0];
    final String input1FilePath = args[1];
    final String outputFilePath = args[2];

    final Pattern pattern = Pattern.compile(" *\\d+ +[0-9.]+ +([0-9.]+) -> ([0-9.]+) +.*Len=(\\d+)");

    final long start = System.currentTimeMillis();

    final SparkSession spark = SparkSession
        .builder()
        .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
        .appName("JavaJoin")
        .getOrCreate();

    final Function<String, Boolean> filterFunction = new Function<String, Boolean>() {
      @Override
      public Boolean call(final String line) throws Exception {
        final Matcher matcher = pattern.matcher(line);
        //System.err.println(matcher);
        //System.err.println(matcher.groupCount());
        //if (matcher.groupCount() == 3) {
        //  System.err.println(matcher.group(1) + ", " + matcher.group(2) + ", " + matcher.group(3));
        //}
        return matcher.find();
      }
    };
    final PairFunction<Tuple2<String, Iterable<Tuple2<String,Long>>>, String, Double> mapToPairFunction =
        new PairFunction<Tuple2<String, Iterable<Tuple2<String, Long>>>, String, Double>() {
          @Override
          public Tuple2<String, Double> call(final Tuple2<String, Iterable<Tuple2<String, Long>>> t) throws Exception {
            return new Tuple2<>(t._1(), stdev(t._2()));
          }
        };

    final SparkContext sparkContext = spark.sparkContext();
    final JavaRDD<String> in0 = (new JavaSparkContext(sparkContext)).textFile(input0FilePath);
    final JavaPairRDD<String, Double> processed0 = in0
        .filter(filterFunction)
        .mapToPair(new PairFunction<String, String, Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Tuple2<String, Long>> call(final String line) throws Exception {
            final Matcher matcher = pattern.matcher(line);
            matcher.find();
            return new Tuple2<>(matcher.group(2), new Tuple2<>(
                matcher.group(1), Long.valueOf(matcher.group(3))));
          }
        })
        .groupByKey()
        .mapToPair(mapToPairFunction);

    final JavaRDD<String> in1 = (new JavaSparkContext(sparkContext)).textFile(input1FilePath);
    final JavaPairRDD<String, Double> processed1 = in1
        .filter(filterFunction)
        .mapToPair(new PairFunction<String, String, Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Tuple2<String, Long>> call(final String line) throws Exception {
            final Matcher matcher = pattern.matcher(line);
            matcher.find();
            return new Tuple2<>(matcher.group(1), new Tuple2<>(
                matcher.group(2), Long.valueOf(matcher.group(3))));
          }
        })
        .groupByKey()
        .mapToPair(mapToPairFunction);

    final JavaPairRDD<String, Tuple2<Iterable<Double>, Iterable<Double>>> joined = processed0.cogroup(processed1);
    final JavaRDD<String> toWrite = joined
        .map(tuple -> tuple._1 + "," + getDouble(tuple._2._1) + "," + getDouble(tuple._2._2));
    toWrite.saveAsTextFile(outputFilePath);

    // DONE
    System.out.println("*******END*******");
    System.out.println("JCT(ms): " + (System.currentTimeMillis() - start));

    spark.stop();
  }

  private static double getDouble(final Iterable<Double> data) {
    for (final double datum : data) {
      return datum;
    }
    return Double.NaN;
  }

  private static double stdev(final Iterable<Tuple2<String, Long>> data) {
    long num = 0;
    long sum = 0;
    long squareSum = 0;
    for (final Tuple2<String, Long> e : data) {
      final long element = e._2();
      num++;
      sum += element;
      squareSum += (element * element);
    }
    if (num == 0) {
      return Double.NaN;
    }
    final double average = ((double) sum) / num;
    final double squareAverage = ((double) squareSum) / num;
    return Math.sqrt(squareAverage - average * average);
  }
}
