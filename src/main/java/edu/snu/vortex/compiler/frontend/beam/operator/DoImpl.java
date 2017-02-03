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
package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.frontend.beam.element.Element;
import edu.snu.vortex.compiler.frontend.beam.element.Record;
import edu.snu.vortex.compiler.frontend.beam.element.Watermark;
import edu.snu.vortex.compiler.ir.operator.Do;
import edu.snu.vortex.compiler.frontend.beam.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.Map;

public final class DoImpl<I, O> extends Do<Element<I>, Element<O>, PCollectionView> {
  private final DoFn doFn;

  public DoImpl(final DoFn doFn) {
    this.doFn = doFn;
  }

  @Override
  public Iterable<Element<O>> transform(final Iterable<Element<I>> input,
                                        final Map<PCollectionView, Object> broadcasted) {
    final DoFnInvoker<I, O> invoker = DoFnInvokers.invokerFor(doFn);
    final ArrayList<Element<O>> outputList = new ArrayList<>();
    final ProcessContext<I, O> context = new ProcessContext<>(doFn, outputList, broadcasted);
    invoker.invokeSetup();
    invoker.invokeStartBundle(context);
    input.forEach(element -> {
      if (element.isWatermark()) {
        outputList.add((Watermark<O>)element.asWatermark());
      } else {
        context.setWindowedValue(element.asRecord().getWindowedValue());
        invoker.invokeProcessElement(context);
      }
    });
    invoker.invokeFinishBundle(context);
    invoker.invokeTeardown();
    return outputList;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", doFn: ");
    sb.append(doFn);
    return sb.toString();
  }
}
