/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.processor.strategy;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static org.mule.runtime.api.i18n.I18nMessageFactory.createStaticMessage;
import static org.mule.runtime.core.api.processor.ReactiveProcessor.ProcessingType.BLOCKING;
import static org.mule.runtime.core.api.transaction.TransactionCoordination.isTransactionActive;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.FluxSink.OverflowStrategy.BUFFER;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.runtime.core.api.MuleContext;
import org.mule.runtime.core.api.construct.BackPressureReason;
import org.mule.runtime.core.api.construct.FlowConstruct;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.api.processor.ReactiveProcessor.ProcessingType;
import org.mule.runtime.core.api.processor.Sink;
import org.mule.runtime.core.api.processor.strategy.ProcessingStrategy;
import org.mule.runtime.core.internal.util.rx.ConditionalExecutorServiceDecorator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

import reactor.core.publisher.EmitterProcessor;

/**
 * Creates {@link ReactorProcessingStrategyFactory.ReactorProcessingStrategy} instance that implements the proactor pattern by
 * de-multiplexing incoming events onto a multiple emitter using the {@link SchedulerService#cpuLightScheduler()} to process these
 * events from each emitter. In contrast to the {@link ReactorStreamProcessingStrategy} the proactor pattern treats
 * {@link ProcessingType#CPU_INTENSIVE} and {@link ProcessingType#BLOCKING} processors differently and schedules there execution
 * on dedicated {@link SchedulerService#cpuIntensiveScheduler()} and {@link SchedulerService#ioScheduler()} ()} schedulers.
 * <p/>
 * This processing strategy is not suitable for transactional flows and will fail if used with an active transaction.
 *
 * @since 4.2.0
 */
public class IOAfterPartyStreamEmitterProcessingStrategyFactory extends ReactorStreamProcessingStrategyFactory {

  @Override
  public ProcessingStrategy create(MuleContext muleContext, String schedulersNamePrefix) {
    return new IOAfterPartyStreamEmitterProcessingStrategy(getBufferSize(),
                                                           getSubscriberCount(),
                                                           getNonBlockingAfterPartyScheduler(muleContext, schedulersNamePrefix),
                                                           resolveParallelism(),
                                                           getMaxConcurrency(),
                                                           isMaxConcurrencyEagerCheck(),
                                                           muleContext.getConfiguration().isThreadLoggingEnabled());
  }

  private Supplier<Scheduler> getNonBlockingAfterPartyScheduler(MuleContext muleContext, String schedulersNamePrefix) {
    return () -> muleContext.getSchedulerService().ioScheduler(muleContext.getSchedulerBaseConfig()
        .withName(schedulersNamePrefix + "." + BLOCKING.name())
        .withMaxConcurrentTasks(CORES * 2));
  }


  @Override
  protected int resolveParallelism() {
    return Integer.max(CORES, getMaxConcurrency());
  }

  @Override
  public Class<? extends ProcessingStrategy> getProcessingStrategyType() {
    return IOAfterPartyStreamEmitterProcessingStrategy.class;
  }

  static class IOAfterPartyStreamEmitterProcessingStrategy extends AbstractReactorStreamProcessingStrategy {

    private final int bufferSize;
    private final boolean isThreadLoggingEnabled;

    public IOAfterPartyStreamEmitterProcessingStrategy(int bufferSize,
                                                       int subscriberCount,
                                                       Supplier<Scheduler> blockingSchedulerSupplier,
                                                       int parallelism,
                                                       int maxConcurrency, boolean maxConcurrencyEagerCheck,
                                                       boolean isThreadLoggingEnabled) {
      super(subscriberCount, blockingSchedulerSupplier, parallelism, maxConcurrency, maxConcurrencyEagerCheck);
      this.bufferSize = requireNonNull(bufferSize);
      this.isThreadLoggingEnabled = isThreadLoggingEnabled;
    }

    public IOAfterPartyStreamEmitterProcessingStrategy(int bufferSize,
                                                       int subscriberCount,
                                                       Supplier<Scheduler> blockingSchedulerSupplier,
                                                       int parallelism,
                                                       int maxConcurrency, boolean maxConcurrencyEagerCheck) {
      this(bufferSize, subscriberCount, blockingSchedulerSupplier, parallelism, maxConcurrency, maxConcurrencyEagerCheck, false);
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

    @Override
    protected ScheduledExecutorService decorateScheduler(ScheduledExecutorService scheduler) {
      return new ConditionalExecutorServiceDecorator(super.decorateScheduler(scheduler),
                                                     currentScheduler -> isTransactionActive());
    }

    @Override
    public ReactiveProcessor onProcessor(ReactiveProcessor processor) {
      return super.onProcessor(processor);
    }

    private int getBufferQueueSize() {
      return bufferSize / (maxConcurrency < CORES ? maxConcurrency : CORES);
    }
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
