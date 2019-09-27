/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.processor.strategy;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static org.mule.runtime.api.i18n.I18nMessageFactory.createStaticMessage;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.FluxSink.OverflowStrategy.BUFFER;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.runtime.core.api.MuleContext;
import org.mule.runtime.core.api.construct.BackPressureReason;
import org.mule.runtime.core.api.construct.FlowConstruct;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.api.processor.Sink;
import org.mule.runtime.core.api.processor.strategy.ProcessingStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

import reactor.core.publisher.EmitterProcessor;

public class StreamEmitterProcessingStrategyFactory extends AbstractStreamProcessingStrategyFactory {

  @Override
  public ProcessingStrategy create(MuleContext muleContext, String schedulersNamePrefix) {
    return new StreamEmitterProcessingStrategy(getBufferSize(),
                                               getSubscriberCount(),
                                               getCpuLightSchedulerSupplier(
                                                                            muleContext,
                                                                            schedulersNamePrefix),
                                               resolveParallelism(),
                                               getMaxConcurrency(),
                                               isMaxConcurrencyEagerCheck());
  }

  @Override
  public Class<? extends ProcessingStrategy> getProcessingStrategyType() {
    return StreamEmitterProcessingStrategy.class;
  }


  static class StreamEmitterProcessingStrategy extends AbstractReactorStreamProcessingStrategy {

    private final int bufferSize;

    public StreamEmitterProcessingStrategy(int bufferSize,
                                           int subscribers,
                                           Supplier<Scheduler> cpuLightSchedulerSupplier,
                                           int parallelism,
                                           int maxConcurrency,
                                           boolean maxConcurrencyEagerCheck) {
      super(subscribers, cpuLightSchedulerSupplier, parallelism, maxConcurrency, maxConcurrencyEagerCheck);
      this.bufferSize = bufferSize;
    }

    @Override
    public Sink createSink(FlowConstruct flowConstruct, ReactiveProcessor function) {
      final long shutdownTimeout = flowConstruct.getMuleContext().getConfiguration().getShutdownTimeout();
      final int sinksCount = maxConcurrency < CORES ? maxConcurrency : CORES;
      List<ReactorSink<CoreEvent>> sinks = new ArrayList<>();

      for (int i = 0; i < sinksCount; i++) {
        Latch completionLatch = new Latch();
        EmitterProcessor<CoreEvent> processor = EmitterProcessor.create(getBufferQueueSize());
        processor.transform(function).subscribe(null, e -> completionLatch.release(), () -> completionLatch.release());

        if (!processor.hasDownstreams()) {
          throw new MuleRuntimeException(createStaticMessage("No subscriptions active for processor."));
        }

        ReactorSink<CoreEvent> sink =
            new DefaultReactorSink<>(processor.sink(BUFFER),
                                     () -> awaitSubscribersCompletion(flowConstruct, shutdownTimeout, completionLatch,
                                                                      currentTimeMillis()),
                                     createOnEventConsumer(), getBufferQueueSize());
        sinks.add(sink);
      }

      return new RoundRobinReactorSink<>(sinks);
    }


    @Override
    public ReactiveProcessor onPipeline(ReactiveProcessor pipeline) {
      reactor.core.scheduler.Scheduler scheduler = fromExecutorService(decorateScheduler(getCpuLightScheduler()));
      return publisher -> from(publisher).publishOn(scheduler)
          .doOnSubscribe(subscription -> currentThread().setContextClassLoader(executionClassloader)).transform(pipeline);
    }

    protected Scheduler getFlowDispatcherScheduler() {
      //TODO: check cap system property
      return getCpuLightScheduler();
    }

    @Override
    protected int getBufferQueueSize() {
      return bufferSize / (maxConcurrency < CORES ? maxConcurrency : CORES);
    }

    static class RoundRobinReactorSink<E> implements AbstractProcessingStrategy.ReactorSink<E> {

      private final List<AbstractProcessingStrategy.ReactorSink<E>> fluxSinks;
      private final AtomicInteger index = new AtomicInteger(0);
      // Saving update function to avoid creating the lambda every time
      private final IntUnaryOperator update;

      public RoundRobinReactorSink(List<AbstractProcessingStrategy.ReactorSink<E>> sinks) {
        this.fluxSinks = sinks;
        this.update = (value) -> (value + 1) % fluxSinks.size();
      }

      @Override
      public void dispose() {
        fluxSinks.stream().forEach(sink -> sink.dispose());
      }

      @Override
      public void accept(CoreEvent event) {
        fluxSinks.get(nextIndex()).accept(event);
      }

      private int nextIndex() {
        return index.getAndUpdate(update);
      }

      @Override
      public BackPressureReason emit(CoreEvent event) {
        return fluxSinks.get(nextIndex()).emit(event);
      }

      @Override
      public E intoSink(CoreEvent event) {
        return (E) event;
      }
    }
  }
}
