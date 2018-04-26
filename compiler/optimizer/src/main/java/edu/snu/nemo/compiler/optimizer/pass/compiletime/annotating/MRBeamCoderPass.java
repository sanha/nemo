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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import scala.Tuple2;

/**
 * Pass for initiating IREdge UsedDataHandling ExecutionProperty with default values.
 */
public final class MRBeamCoderPass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public MRBeamCoderPass() {
    super(ExecutionProperty.Key.Compression, Collections.emptySet());
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final List<IRVertex> irVertexList = dag.getTopologicalSort();
    int edgeIdx = 1;

    for (final IRVertex irVertex : irVertexList) {
      final List<IREdge> edges = dag.getOutgoingEdgesOf(irVertex);
      if (edges.size() > 1) {
        throw new RuntimeException("many edges!");
      } else if (edges.size() == 1) {
        final IREdge edge = edges.get(0);
        switch (edgeIdx) {
          case 1:
          case 4:
            edge.setCoder(new CustomBeamCoder(StringUtf8Coder.of()));
            break;
          case 2:
          case 3:
            edge.setCoder(new CustomBeamCoder(new Tuple2Coder<>(StringUtf8Coder.of(), VarLongCoder.of())));
            break;
          default:
            throw new RuntimeException("@@@@@@@@@@@@@ many edges!");
        }
        edgeIdx++;
      }
    }

    return dag;
  }

  /**
   * {@link Coder} from {@link org.apache.beam.sdk.coders.Coder}.
   *
   * @param <T> element type.
   */
  private final class CustomBeamCoder<T> implements Coder<T> {
    private final org.apache.beam.sdk.coders.Coder<T> beamCoder;

    /**
     * Constructor of BeamCoder.
     *
     * @param beamCoder actual Beam coder to use.
     */
    private CustomBeamCoder(final org.apache.beam.sdk.coders.Coder<T> beamCoder) {
      this.beamCoder = beamCoder;
    }

    @Override
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
    }

    @Override
    public String toString() {
      return beamCoder.toString();
    }
  }

  /**
   * A {@code KvCoder} encodes {@link Tuple2}s.
   *
   * @param <K> the type of the keys of the KVs being transcoded
   * @param <V> the type of the values of the KVs being transcoded
   */
  public static class Tuple2Coder<K, V> extends org.apache.beam.sdk.coders.Coder<Tuple2<K, V>> {
    public static <K, V> Tuple2Coder<K, V> of(org.apache.beam.sdk.coders.Coder<K> keyCoder,
                                              org.apache.beam.sdk.coders.Coder<V> valueCoder) {
      return new Tuple2Coder<>(keyCoder, valueCoder);
    }

    public org.apache.beam.sdk.coders.Coder<K> getKeyCoder() {
      return keyCoder;
    }

    public org.apache.beam.sdk.coders.Coder<V> getValueCoder() {
      return valueCoder;
    }

    /////////////////////////////////////////////////////////////////////////////

    private final org.apache.beam.sdk.coders.Coder<K> keyCoder;
    private final org.apache.beam.sdk.coders.Coder<V> valueCoder;

    private Tuple2Coder(final org.apache.beam.sdk.coders.Coder<K> keyCoder,
                        final org.apache.beam.sdk.coders.Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public void encode(Tuple2<K, V> tuple, OutputStream outStream)
        throws IOException, CoderException {
      if (tuple == null) {
        throw new CoderException("cannot encode a null KV");
      }
      keyCoder.encode(tuple._1(), outStream);
      valueCoder.encode(tuple._2(), outStream);
    }

    @Override
    public Tuple2<K, V> decode(InputStream inStream) throws IOException, CoderException {
      K key = keyCoder.decode(inStream);
      V value = valueCoder.decode(inStream);
      return new Tuple2<>(key, value);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "Key coder must be deterministic", getKeyCoder());
      verifyDeterministic(this, "Value coder must be deterministic", getValueCoder());
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return Arrays.asList(keyCoder, valueCoder);
    }
  }
}
