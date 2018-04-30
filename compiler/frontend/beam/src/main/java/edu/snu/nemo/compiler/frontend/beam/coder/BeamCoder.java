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
package edu.snu.nemo.compiler.frontend.beam.coder;

import edu.snu.nemo.common.coder.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * {@link Coder} from {@link org.apache.beam.sdk.coders.Coder}.
 * @param <T> element type.
 */
public final class BeamCoder<T> implements Coder<T> {
  private final org.apache.beam.sdk.coders.Coder<T> beamCoder;

  /**
   * Constructor of BeamCoder.
   * @param beamCoder actual Beam coder to use.
   */
  public BeamCoder(final org.apache.beam.sdk.coders.Coder<T> beamCoder) {
    this.beamCoder = beamCoder;
  }

  /*@Override
  public void encode(final T value, final OutputStream outStream) throws IOException {
    if (beamCoder instanceof VoidCoder) {
      outStream.write(0);
      return;
    }
    try {
      beamCoder.encode(value, outStream);
    } catch (final CoderException e) {
      throw new IOException(e);
    }
  }

  @Override
  public T decode(final InputStream inStream) throws IOException {
    if (beamCoder instanceof VoidCoder && inStream.read() == -1) {
      throw new IOException("End of stream reached");
    }
    try {
      return beamCoder.decode(inStream);
    } catch (final CoderException e) {
      throw new IOException(e);
    }
  }*/

  @Override
  public String toString() {
    return beamCoder.toString();
  }

  @Override
  public EncoderInstance getEncoderInstance(final OutputStream outputStream) {
    return new BeamEncoderInstance(outputStream, beamCoder);
  }

  @Override
  public DecoderInstance getDecoderInstance(final InputStream inputStream) {
    return new BeamDecoderInstance(inputStream, beamCoder);
  }

  /**
   * BeamEncoderInstance.
   */
  private final class BeamEncoderInstance implements EncoderInstance<T> {

    private final org.apache.beam.sdk.coders.Coder<T> beamCoder;
    private final OutputStream out;

    private BeamEncoderInstance(final OutputStream outputStream,
                                final org.apache.beam.sdk.coders.Coder<T> beamCoder) {
      this.out = outputStream;
      this.beamCoder = beamCoder;
    }

    public void encode(final T element) throws IOException {
      if (beamCoder instanceof VoidCoder) {
        out.write(0);
        return;
      }
      try {
        beamCoder.encode(element, out);
      } catch (final CoderException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * BeamDecoderInstance.
   */
  private final class BeamDecoderInstance implements DecoderInstance<T> {

    private final org.apache.beam.sdk.coders.Coder<T> beamCoder;
    private final InputStream in;

    private BeamDecoderInstance(final InputStream inputStream,
                                final org.apache.beam.sdk.coders.Coder<T> beamCoder) {
      this.in = inputStream;
      this.beamCoder = beamCoder;
    }

    public T decode() throws IOException {
      if (beamCoder instanceof VoidCoder && in.read() == -1) {
        throw new IOException("End of stream reached");
      }
      try {
        return beamCoder.decode(in);
      } catch (final CoderException e) {
        throw new IOException(e);
      }
    }
  }
}
