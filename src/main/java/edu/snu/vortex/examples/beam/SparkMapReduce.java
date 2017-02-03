/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.examples.beam;

import kafka.serializer.StringDecoder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class SparkMapReduce {
  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 2) {
      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }

    final String brokers = args[0];
    final String topics = args[1];
    final Long minibatchSize = Long.valueOf(args[2]);
    final Long windowSize = Long.valueOf(args[3]);
    final Long slideDuration = Long.valueOf(args[4]);

    final SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
    final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(minibatchSize));

    final Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
    final Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", brokers);

    // Create direct kafka stream with brokers and topics
    final JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicsSet
    );

    final JavaPairDStream<String, Long> pairs = messages
        .map(tuple -> tuple._2())
        .mapToPair(msg -> {
          final String[] splits = msg.split(" ");
          return new Tuple2<>(splits[0], Long.valueOf(splits[3]));
        });

    final JavaPairDStream<String, Long> sum =
        pairs.reduceByKeyAndWindow((l, r) -> (l + r), Durations.seconds(windowSize), Durations.seconds(slideDuration));

    final JavaPairDStream<Text, LongWritable> writable =
        sum.mapToPair(tuple -> new Tuple2<>(new Text(tuple._1()), new LongWritable(tuple._2())));

    writable.saveAsNewAPIHadoopFiles("hdfs://rio-m:9000/starlab/" + System.currentTimeMillis(), "starlab", Text.class, LongWritable.class, TextOutputFormat.class);

    jssc.start();
    jssc.awaitTermination();
  }
}