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
package edu.snu.nemo.compiler.frontend.spark.coder;

import edu.snu.nemo.common.coder.Coder;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Kryo Spark Coder for serialization.
 * @param <T> type of the object to (de)serialize.
 */
public final class SparkCoder<T> implements Coder<T> {
  private final Serializer serializer;
  @Nullable private volatile SerializerInstance serInstance;

  /**
   * Default constructor.
   * @param serializer spark serializer.
   */
  public SparkCoder(final Serializer serializer) {
    this.serializer = serializer;
    this.serInstance = null; // SerializerInstance is not Serializable.
  }

  /*@Override
  public void encode(final T element, final OutputStream outStream) throws IOException {
    if (serInstance == null) {
      serInstance = serializer.newInstance();
    }
    serInstance.serializeStream(outStream).writeObject(element, ClassTag$.MODULE$.Any());
  }

  @Override
  public T decode(final InputStream inStream) throws IOException {
    if (serInstance == null) {
      serInstance = serializer.newInstance();
    }
    final T obj = (T) serInstance.deserializeStream(inStream).readObject(ClassTag$.MODULE$.Any());
    return obj;
  }*/

  @Override
  public EncoderInstance getEncoderInstance(final OutputStream outputStream) {
    if (serInstance == null) {
      serInstance = serializer.newInstance();
    }
    return new SparkEncoderInstance(outputStream, serInstance);
  }

  @Override
  public DecoderInstance getDecoderInstance(final InputStream inputStream) {
    if (serInstance == null) {
      serInstance = serializer.newInstance();
    }
    return new SparkDecoderInstance(inputStream, serInstance);
  }

  /**
   * SparkEncoderInstance.
   */
  private final class SparkEncoderInstance implements EncoderInstance<T> {

    private final SerializationStream out;

    private SparkEncoderInstance(final OutputStream outputStream,
                                 final SerializerInstance sparkSerializerInstance) {
      this.out = sparkSerializerInstance.serializeStream(outputStream);
    }

    public void encode(final T element) throws IOException {
      out.writeObject(element, ClassTag$.MODULE$.Any());
    }
  }

  /**
   * SparkDecoderInstance.
   */
  private final class SparkDecoderInstance implements DecoderInstance<T> {

    private final DeserializationStream in;

    private SparkDecoderInstance(final InputStream inputStream,
                                 final SerializerInstance sparkSerializerInstance) {
      this.in = sparkSerializerInstance.deserializeStream(inputStream);
    }

    public T decode() throws IOException {
      final T obj = (T) in.readObject(ClassTag$.MODULE$.Any());
      return obj;
    }
  }

  @Override
  public String toString() {
    return serializer.toString();
  }
}
