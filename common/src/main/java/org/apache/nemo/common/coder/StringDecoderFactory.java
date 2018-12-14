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
import org.apache.commons.lang.SerializationUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * A {@link DecoderFactory} which is used for long.
 */
public final class StringDecoderFactory implements DecoderFactory<String> {

  private static final StringDecoderFactory STRING_DECODER_FACTORY = new StringDecoderFactory();

  /**
   * A private constructor.
   */
  private StringDecoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the coder.
   */
  public static StringDecoderFactory of() {
    return STRING_DECODER_FACTORY;
  }

  @Override
  public Decoder<String> create(final InputStream inputStream) {
    return new StringDecoder(inputStream);
  }

  /**
   * StringDecoder.
   */
  private final class StringDecoder implements Decoder<String> {
    private final DataInputStream inputStream;
    private final StringUtf8Coder coder;

    /**
     * Constructor.
     *
     * @param inputStream  the input stream to decode.
     */
    private StringDecoder(final InputStream inputStream) {
      // If the inputStream is closed well in upper level, it is okay to not close this stream
      // because the DataInputStream itself will not contain any extra information.
      // (when we close this stream, the input will be closed together.)
      this.inputStream = new DataInputStream(inputStream);
      this.coder = StringUtf8Coder.of();
    }

    @Override
    public String decode() throws IOException {
      return coder.decode(inputStream);
    }
  }
}
