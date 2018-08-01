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
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Random;

/**
 * WordCount application.
 */
public final class GenerateMidHotKey {
  /**
   * Private Constructor.
   */
  private GenerateMidHotKey() {
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
    options.setJobName("GenerateMidHotKey");

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.<String, String>via(new SimpleFunction<String, String>() {
          @Override
          public String apply(final String line) {
            final String[] words = line.split(" ");
            final String value = words[1];

            final String newKey;

            final Random random = new Random(System.nanoTime());
            //final int mod = Math.abs(random.nextInt() % 100);
            final int mod = Math.abs(random.nextInt() % 67); // 20 key 30%

            if (mod < 1) {
              newKey = "111.111";
            } else if (mod < 2) {
              newKey = "222.222";
            } else if (mod < 3) {
              newKey = "333.333";
            } else if (mod < 4) {
              newKey = "444.444";
            } else if (mod < 5) {
              newKey = "555.555";
            } else if (mod < 6) {
              newKey = "666.666";
            } else if (mod < 7) {
              newKey = "777.777";
            } else if (mod < 8) {
              newKey = "888.888";
            } else if (mod < 9) {
              newKey = "999.999";
            } else if (mod < 10) {
              newKey = "1000.000";
            } else if (mod < 11) {
              newKey = "1100.000";
            } else if (mod < 12) {
              newKey = "1200.000";
            } else if (mod < 13) {
              newKey = "1300.000";
            } else if (mod < 14) {
              newKey = "1400.000";
            } else if (mod < 15) {
              newKey = "1500.000";
            } else if (mod < 16) {
              newKey = "1600.000";
            } else if (mod < 17) {
              newKey = "1700.000";
            } else if (mod < 18) {
              newKey = "1800.000";
            } else if (mod < 19) {
              newKey = "1900.000";
            } else if (mod < 20) {
              newKey = "2000.000";
            } else {
              newKey = RandomStringUtils.randomAlphanumeric(8);
            }

            return newKey + " " + value;
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }
}
