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
package edu.snu.nemo.common.test;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test Utils for Examples.
 */
public final class ExampleTestUtil {
  private static final Double ERROR = 1e-8;
  /**
   * Private constructor.
   */
  private ExampleTestUtil() {
  }

  public static void genTest() {
    long count = -1;
    try {
      count = Files.lines(Paths.get("/Users/sanha/bb2e4b02-e402-4c2e-bd6c-016143a2cb00"))
          .filter(line -> line.contains("222.222"))
          .count();
    } catch (final Exception e) {

    }
    System.out.println(count);
  }

  public static void hashTest() {
    final int mod = new BigInteger(String.valueOf(Math.round(989 * 1.5))).nextProbablePrime().intValue();
    System.out.println("mod is " + mod);
    System.out.println(String.valueOf(Math.round(989 * 1.5)));
    final ArrayList<String> keys = new ArrayList<>(20);
    for (int i = 1; i <= 9; i++) {
      System.out.print("keys.add(\"");
      for (int j = 0; j < 3; j++) {
        System.out.print(i);
      }
      System.out.print(".");
      for (int j = 0; j < 3; j++) {
        System.out.print(i);
      }
      System.out.println("\");");
    }
    for (int i = 10; i <= 20; i++) {
      System.out.print("keys.add(\"");
      System.out.print(i);
      System.out.println("00.000\");");
    }
    keys.add("111.111");
    keys.add("222.222");
    keys.add("333.333");
    keys.add("444.444");
    keys.add("555.555");
    keys.add("666.666");
    keys.add("777.777");
    keys.add("888.888");
    keys.add("999.999");
    keys.add("1000.000");
    keys.add("1100.000");
    keys.add("1200.000");
    keys.add("1300.000");
    keys.add("1400.000");
    keys.add("1500.000");
    keys.add("1600.000");
    keys.add("1700.000");
    keys.add("1800.000");
    keys.add("1900.000");
    keys.add("2000.000");

    final List<Integer> indices = new ArrayList<>(20);
    for (int i = 0; i < 20; i++) {
      indices.add(Math.abs(keys.get(i).hashCode() % mod));
    }
    indices.sort(Integer::compareTo);
    System.out.println(indices);

    double prob = 1;
    for (int i = 1; i <= 20; i++) {
      prob = prob * (2000 - i) / 2000;
    }

    System.out.println(prob);
  }

  public static void tmpEnsureOutputValidity() throws IOException {
    //final String expectedFilePath = "/Users/sanha/pagecounts-20160101-000000-wc.out";
    final String actualOutputDirectory = "/Users/sanha/pagecounts-20160101-000000-wcskew.out/";
    final String concatOutputPath = "/Users/sanha/pagecounts-20160101-000000-wcskew-concat.out";

    final ExecutorService executorService = Executors.newFixedThreadPool(8);

    final ArrayList<String> testOutput = new ArrayList<>();
    final List<ArrayList<String>> testOutputPerThread = new CopyOnWriteArrayList<>();
    final ArrayList<Future> futures = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      final int threadIdx = i;
      futures.add(executorService.submit(() -> {
        final ArrayList<String> testOutputForThisThread = new ArrayList<>();
        testOutputPerThread.add(testOutputForThisThread);
        try (final Stream<Path> fileStream = Files.list(Paths.get(actualOutputDirectory))) {
          final List<Path> paths = fileStream.collect(Collectors.toList());
          for (int fileIdx = 0; fileIdx < paths.size(); fileIdx++) {
            if (fileIdx % 8 == threadIdx) {
              final Path pathToConcat = paths.get(fileIdx);
              if (Files.isRegularFile(pathToConcat)) {
                Files.lines(pathToConcat).forEach(testOutputForThisThread::add);
              }
            }
          }
          testOutputForThisThread.sort(String::compareTo);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }));
    }

    futures.forEach(future -> {
          try {
            synchronized (future) {
              future.wait();
            }
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        });

    testOutputPerThread.forEach(testOutputForThread -> {
      testOutput.addAll(testOutputForThread);
    });

    testOutput.sort(String::compareTo);

    final String concatOutput = testOutput.stream()
        .reduce("", (p, q) -> (p + "\n" + q));

    try (final PrintWriter writer = new PrintWriter(concatOutputPath, "UTF-8");) {
      writer.print(concatOutput);
    }
  }


    /**
     * Ensures output correctness with the given test resource file.
     *
     * @param resourcePath root folder for both resources.
     * @param outputFileName output file name.
     * @param testResourceFileName the test result file name.
     * @throws RuntimeException if the output is invalid.
     */
  public static void ensureOutputValidity(final String resourcePath,
                                          final String outputFileName,
                                          final String testResourceFileName) throws IOException {

    final String testOutput;
    try (final Stream<Path> fileStream = Files.list(Paths.get(resourcePath))) {
      testOutput = fileStream
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().startsWith(outputFileName))
          .flatMap(path -> {
            try {
              return Files.lines(path);
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          })
          .sorted()
          .reduce("", (p, q) -> (p + "\n" + q));
    }

    final String resourceOutput;

    try (final Stream<String> lineStream = Files.lines(Paths.get(resourcePath + testResourceFileName))) {
      resourceOutput = lineStream
          .sorted()
          .reduce("", (p, q) -> (p + "\n" + q));
    }

    if (!testOutput.equals(resourceOutput)) {
      final String outputMsg =
          "Test output mismatch while comparing [" + outputFileName + "] from [" + testResourceFileName + "] under "
              + resourcePath + ":\n"
              + "=============" + outputFileName + "=================="
              + testOutput
              + "\n=============" + testResourceFileName + "=================="
              + resourceOutput
              + "\n===============================";
      throw new RuntimeException(outputMsg);
    }
  }

  /**
   * This method test the output validity of AlternatingLeastSquareITCase.
   * Due to the floating point math error, the output of the test can be different every time.
   * Thus we cannot compare plain text output, but have to check its numeric error.
   *
   * @param resourcePath path to resources.
   * @param outputFileName name of output file.
   * @param testResourceFileName name of the file to compare the outputs to.
   * @throws RuntimeException if the output is invalid.
   * @throws IOException exception.
   */
  public static void ensureALSOutputValidity(final String resourcePath,
                                             final String outputFileName,
                                             final String testResourceFileName) throws IOException {

    final List<List<Double>> testOutput;
    try (final Stream<Path> fileStream = Files.list(Paths.get(resourcePath))) {
      testOutput = fileStream
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().startsWith(outputFileName))
          .flatMap(path -> {
            try {
              return Files.lines(path);
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          })
          .sorted()
          .filter(line -> !line.trim().equals(""))
          .map(line -> Arrays.asList(line.split("\\s*,\\s*"))
              .stream().map(s -> Double.valueOf(s)).collect(Collectors.toList()))
          .collect(Collectors.toList());
    }

    final List<List<Double>> resourceOutput;
    try (final Stream<String> lineStream = Files.lines(Paths.get(resourcePath + testResourceFileName))) {
      resourceOutput = lineStream
          .sorted()
          .filter(line -> !line.trim().equals(""))
          .map(line -> Arrays.asList(line.split("\\s*,\\s*"))
              .stream().map(s -> Double.valueOf(s)).collect(Collectors.toList()))
          .collect(Collectors.toList());
    }

    if (testOutput.size() != resourceOutput.size()) {
      throw new RuntimeException("output mismatch");
    }

    for (int i = 0; i < testOutput.size(); i++) {
      for (int j = 0; j < testOutput.get(i).size(); j++) {
        final Double testElement = testOutput.get(i).get(j);
        final Double resourceElement = resourceOutput.get(i).get(j);
        if (Math.abs(testElement - resourceElement) / resourceElement > ERROR) {
          throw new RuntimeException("output mismatch");
        }
      }
    }
  }

  /**
   * Delete output files.
   *
   * @param directory      the path of file directory.
   * @param outputFileName the output file prefix.
   * @throws IOException if fail to delete.
   */
  public static void deleteOutputFile(final String directory,
                                      final String outputFileName) throws IOException {
    try (final Stream<Path> fileStream = Files.list(Paths.get(directory))) {
      final Set<Path> outputFilePaths = fileStream
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().startsWith(outputFileName))
          .collect(Collectors.toSet());
      for (final Path outputFilePath : outputFilePaths) {
        Files.delete(outputFilePath);
      }
    }
  }
}
