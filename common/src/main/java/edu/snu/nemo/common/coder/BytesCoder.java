/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.nemo.common.coder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * A {@link Coder} which is used for an array of bytes.
 */
public final class BytesCoder implements Coder<byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(BytesCoder.class.getName());

  /**
   * Constructor.
   */
  public BytesCoder() {
  }

  /**
   * The array of bytes to write has to have the exact length to write.
   */
  /*@Override
  public void encode(final byte[] value, final OutputStream outStream) throws IOException {
    outStream.write(value);
  }

  @Override
  public byte[] decode(final InputStream inStream) throws IOException {
    //LOG.warn("InputStream#available() might not exactly represent the number of bytes to read. "
    //    + "Please use decode(int, InputStream) method with explicit length.");
    //return this.decode(inStream.available(), inStream);
    final int lengthToRead = inStream.available();
    final byte[] bytes = new byte[lengthToRead]; // Read the size of this byte array.
    final int readBytes = inStream.read(bytes);
    if (lengthToRead != readBytes) {
      throw new IOException("Have to read " + lengthToRead + " but read only " + readBytes + " bytes.");
    }
    return bytes;
  }*/

  @Override
  public EncoderInstance getEncoderInstance(final OutputStream outputStream) {
    return new BytesEncoderInstance(outputStream);
  }

  @Override
  public DecoderInstance getDecoderInstance(final InputStream inputStream) {
    return new BytesDecoderInstance(inputStream);
  }

  /**
   * BytesEncoderInstance.
   */
  private final class BytesEncoderInstance implements EncoderInstance<byte[]> {

    private final OutputStream out;

    private BytesEncoderInstance(final OutputStream outputStream) {
      this.out = outputStream;
    }

    public void encode(final byte[] element) throws IOException {
      out.write(element);
    }
  }

  /**
   * BytesDecoderInstance.
   */
  private final class BytesDecoderInstance implements DecoderInstance<byte[]> {

    private final InputStream in;

    private BytesDecoderInstance(final InputStream inputStream) {
      this.in = inputStream;
    }

    public byte[] decode() throws IOException {
      //LOG.warn("InputStream#available() might not exactly represent the number of bytes to read. "
      //    + "Please use decode(int, InputStream) method with explicit length.");
      //return this.decode(inStream.available(), inStream);
      final int lengthToRead = in.available();
      final byte[] bytes = new byte[lengthToRead]; // Read the size of this byte array.
      final int readBytes = in.read(bytes);
      if (lengthToRead != readBytes) {
        throw new IOException("Have to read " + lengthToRead + " but read only " + readBytes + " bytes.");
      }
      return bytes;
    }
  }
}
