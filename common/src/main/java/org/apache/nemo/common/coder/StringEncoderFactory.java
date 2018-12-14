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
package org.apache.nemo.common.coder;

import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link EncoderFactory} which is used for long.
 */
public final class StringEncoderFactory implements EncoderFactory<String> {

  private static final StringEncoderFactory STRING_ENCODER_FACTORY = new StringEncoderFactory();

  /**
   * A private constructor.
   */
  private StringEncoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the coder.
   */
  public static StringEncoderFactory of() {
    return STRING_ENCODER_FACTORY;
  }

  @Override
  public Encoder<String> create(final OutputStream outputStream) {
    return new StringEncoder(outputStream);
  }

  /**
   * LongEncoder.
   */
  private final class StringEncoder implements Encoder<String> {

    private final DataOutputStream outputStream;
    private final StringUtf8Coder coder;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private StringEncoder(final OutputStream outputStream) {
      // If the outputStream is closed well in upper level, it is okay to not close this stream
      // because the DataOutputStream itself will not contain any extra information.
      // (when we close this stream, the output will be closed together.)
      this.outputStream = new DataOutputStream(outputStream);
      this.coder = StringUtf8Coder.of();
    }

    @Override
    public void encode(final String value) throws IOException {
      coder.encode(value, outputStream);
    }
  }
}
