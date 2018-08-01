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
package edu.snu.nemo.examples.beam;

import edu.snu.nemo.compiler.frontend.beam.NemoPipelineOptions;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * IP stat analysis example used in skew experiment.
 */
public final class PerKeyMedianSailfish {
  private static final Logger LOG = LoggerFactory.getLogger(PerKeyMedianSailfish.class.getName());

  /**
   * Private Constructor.
   */
  private PerKeyMedianSailfish() {
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("PerKeyMedian");

    final Pipeline p = Pipeline.create(options);

    long start = System.currentTimeMillis();

    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
          @Override
          public KV<String, String> apply(final String line) {
            final String[] words = line.split(" ");
            String key = words[0];
            String value = words[1];
            return KV.of(key, value);
          }
        }))
        .apply(GroupByKey.create())
        .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {
          @Override
          public String apply(final KV<String, Iterable<String>> kv) {
            final String key = kv.getKey();
            final List<String> value1 = new ArrayList<>();
            final List<String> value2 = new ArrayList<>();

            boolean bool = false;
            for (final String s : kv.getValue()) {
              if (bool) {
                value1.add(s);
              } else {
                value2.add(s);
              }
              bool = !bool;
            }

            String maxLengthMedian = "";
            for (final String s1 : value1) {
              List<String> concatStrings = new ArrayList<>(value2.size());
              value2.forEach(s2 -> concatStrings.add(s1 + s2)); // concat.
              concatStrings.sort(String::compareTo);
              final String maxLengthMedianInSubset = concatStrings.get(concatStrings.size() / 2);
              if (maxLengthMedian.length() < maxLengthMedianInSubset.length()) {
                maxLengthMedian = maxLengthMedianInSubset;
              }
            }

            return maxLengthMedian;
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();

    LOG.info("*******END*******");
    LOG.info("JCT(ms): " + (System.currentTimeMillis() - start));
  }
}
