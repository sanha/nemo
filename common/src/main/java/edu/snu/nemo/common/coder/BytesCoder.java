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

import java.io.*;

/**
 * A {@link Coder} which is used for an array of bytes.
 */
public final class BytesCoder implements Coder<BytesCoder.BytesWrapper> {

  /**
   * Constructor.
   */
  public BytesCoder() {
  }

  @Override
  public void encode(final BytesWrapper value, final OutputStream outStream) throws IOException {
    try (final DataOutputStream dataOutputStream = new DataOutputStream(outStream)) {
      dataOutputStream.writeInt(value.getLength()); // Write the size of this byte array.
      dataOutputStream.writeLong(value.getElementsTotal()); // Write the number of elements in this byte array.
      dataOutputStream.write(value.getBytes());
    }
  }

  @Override
  public BytesWrapper decode(final InputStream inStream) throws IOException {
    // If the inStream is closed well in upper level, it is okay to not close this stream
    // because the DataInputStream itself will not contain any extra information.
    // (when we close this stream, the inStream will be closed together.)
    final DataInputStream dataInputStream = new DataInputStream(inStream);
    final int bytesToRead = dataInputStream.readInt();
    final long elementsTotal = dataInputStream.readLong();
    final byte[] bytes = new byte[bytesToRead]; // Read the size of this byte array.
    final int readBytes = dataInputStream.read(bytes);
    if (bytesToRead != readBytes) {
      throw new IOException("Have to read " + bytesToRead + " but read only " + readBytes + " bytes.");
    }
    return new BytesWrapper(elementsTotal, bytes);
  }

  /**
   * A wrapper class for an array of bytes.
   */
  public final class BytesWrapper {
    private long elementsTotal;
    private byte[] bytes;

    /**
     * Constructor.
     *
     * @param elementsTotal the total number of elements in this array of bytes.
     * @param bytes         the array of bytes.
     */
    public BytesWrapper(final long elementsTotal,
                        final byte[] bytes) {
      this.elementsTotal = elementsTotal;
      this.bytes = bytes;
    }

    /**
     * @return the length of the byte array.
     */
    public int getLength() {
      return bytes.length;
    }

    /**
     * @return the total number of elements in the byte array.
     */
    public long getElementsTotal() {
      return elementsTotal;
    }

    /**
     * @return the wrapped byte array.
     */
    public byte[] getBytes() {
      return bytes;
    }
  }
}
