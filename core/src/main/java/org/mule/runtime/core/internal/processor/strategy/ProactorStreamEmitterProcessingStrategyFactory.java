/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.processor.strategy;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mule.runtime.api.i18n.I18nMessageFactory.createStaticMessage;
import static org.mule.runtime.core.api.construct.BackPressureReason.REQUIRED_SCHEDULER_BUSY;
import static org.mule.runtime.core.api.construct.BackPressureReason.REQUIRED_SCHEDULER_BUSY_WITH_FULL_BUFFER;
import static org.mule.runtime.core.api.processor.ReactiveProcessor.ProcessingType.BLOCKING;
import static org.mule.runtime.core.api.processor.ReactiveProcessor.ProcessingType.CPU_INTENSIVE;
import static org.mule.runtime.core.api.processor.ReactiveProcessor.ProcessingType.IO_RW;
import static org.mule.runtime.core.internal.context.thread.notification.ThreadNotificationLogger.THREAD_NOTIFICATION_LOGGER_CONTEXT_KEY;
import static org.slf4j.LoggerFactory.getLogger;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.FluxSink.OverflowStrategy.BUFFER;
import static reactor.core.publisher.Mono.subscriberContext;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

import org.mule.runtime.api.exception.MuleException;
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
import org.mule.runtime.core.internal.context.thread.notification.ThreadLoggingExecutorServiceDecorator;
import org.mule.runtime.core.internal.processor.chain.InterceptedReactiveProcessor;
import org.mule.runtime.core.internal.util.rx.RejectionCallbackExecutorServiceDecorator;
import org.mule.runtime.core.internal.util.rx.RetrySchedulerWrapper;
import org.mule.runtime.core.privileged.event.BaseEventContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import org.slf4j.Logger;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ProactorStreamEmitterProcessingStrategyFactory extends AbstractStreamProcessingStrategyFactory {

  @Override
  public ProcessingStrategy create(MuleContext muleContext, String schedulersNamePrefix) {
    return new ProactorStreamEmitterProcessingStrategy(getBufferSize(),
                                                       getSubscriberCount(),
                                                       getCpuLightSchedulerSupplier(muleContext, schedulersNamePrefix),
                                                       () -> muleContext.getSchedulerService()
                                                           .ioScheduler(muleContext.getSchedulerBaseConfig()
                                                               .withName(
                                                                         schedulersNamePrefix + "." + BLOCKING.name())),
                                                       () -> muleContext.getSchedulerService()
                                                           .cpuIntensiveScheduler(muleContext.getSchedulerBaseConfig()
                                                               .withName(schedulersNamePrefix + "."
                                                                   + CPU_INTENSIVE.name())),
                                                       resolveParallelism(),
                                                       getMaxConcurrency(),
                                                       isMaxConcurrencyEagerCheck(),
                                                       muleContext.getConfiguration().isThreadLoggingEnabled());
  }

  @Override
  public Class<? extends ProcessingStrategy> getProcessingStrategyType() {
    return ProactorStreamEmitterProcessingStrategy.class;
  }

  static class ProactorStreamEmitterProcessingStrategy extends AbstractReactorStreamProcessingStrategy {

    private static final Logger LOGGER = getLogger(ProactorStreamEmitterProcessingStrategy.class);
    private static final long SCHEDULER_BUSY_RETRY_INTERVAL_NS = MILLISECONDS.toNanos(SCHEDULER_BUSY_RETRY_INTERVAL_MS);

    private static Class<ClassLoader> SDK_OPERATION_CLASS;

    static {
      try {
        SDK_OPERATION_CLASS = (Class<ClassLoader>) ProactorStreamEmitterProcessingStrategy.class.getClassLoader()
            .loadClass("org.mule.runtime.module.extension.internal.runtime.operation.OperationMessageProcessor");
      } catch (ClassNotFoundException e) {
        LOGGER.debug("OperationMessageProcessor interface not available in current context", e);
      }

    }

    private final int bufferSize;
    private final boolean isThreadLoggingEnabled;
    private final Supplier<Scheduler> blockingSchedulerSupplier;
    private final Supplier<Scheduler> cpuIntensiveSchedulerSupplier;
    private final AtomicInteger queuedEvents = new AtomicInteger();
    private final BiConsumer<CoreEvent, Throwable> queuedDecrementCallback = (e, t) -> queuedEvents.decrementAndGet();
    private final LongUnaryOperator lastRetryTimestampCheckOperator =
        v -> nanoTime() - v < SCHEDULER_BUSY_RETRY_INTERVAL_NS * 2
            ? v
            : MIN_VALUE;

    protected final AtomicLong lastRetryTimestamp = new AtomicLong(MIN_VALUE);

    private Scheduler blockingScheduler;
    private Scheduler cpuIntensiveScheduler;

    public ProactorStreamEmitterProcessingStrategy(int bufferSize,
                                                   int subscriberCount,
                                                   Supplier<Scheduler> cpuLightSchedulerSupplier,
                                                   Supplier<Scheduler> blockingSchedulerSupplier,
                                                   Supplier<Scheduler> cpuIntensiveSchedulerSupplier,
                                                   int parallelism,
                                                   int maxConcurrency,
                                                   boolean maxConcurrencyEagerCheck,
                                                   boolean isThreadLoggingEnabled) {
      super(subscriberCount, cpuLightSchedulerSupplier, parallelism, maxConcurrency, maxConcurrencyEagerCheck);
      this.bufferSize = bufferSize;
      this.blockingSchedulerSupplier = blockingSchedulerSupplier;
      this.cpuIntensiveSchedulerSupplier = cpuIntensiveSchedulerSupplier;
      this.isThreadLoggingEnabled = isThreadLoggingEnabled;
    }

    @Override
    public void start() throws MuleException {
      super.start();
      this.blockingScheduler = blockingSchedulerSupplier.get();
      this.cpuIntensiveScheduler = cpuIntensiveSchedulerSupplier.get();
    }

    @Override
    protected Scheduler createCpuLightScheduler(Supplier<Scheduler> cpuLightSchedulerSupplier) {
      return new RetrySchedulerWrapper(super.createCpuLightScheduler(cpuLightSchedulerSupplier),
                                       SCHEDULER_BUSY_RETRY_INTERVAL_MS);
    }

    @Override
    public void stop() throws MuleException {
      super.stop();
      if (blockingScheduler != null) {
        blockingScheduler.stop();
      }
      if (cpuIntensiveScheduler != null) {
        cpuIntensiveScheduler.stop();
      }
    }

    @Override
    public ReactiveProcessor onProcessor(ReactiveProcessor processor) {
      if (processor.getProcessingType() == BLOCKING || processor.getProcessingType() == IO_RW) {
        return proactor(processor, blockingScheduler);
      } else if (processor.getProcessingType() == CPU_INTENSIVE) {
        return proactor(processor, cpuIntensiveScheduler);
      } else {
        return super.onProcessor(processor);
      }
    }

    protected ReactiveProcessor proactor(ReactiveProcessor processor, ScheduledExecutorService scheduler) {
      LOGGER.debug("Doing proactor() for {} on {}. maxConcurrency={}, parallelism={}, subscribers={}", processor, scheduler,
                   maxConcurrency, getParallelism(), subscribers);

      final ScheduledExecutorService retryScheduler =
          new RejectionCallbackExecutorServiceDecorator(scheduler, getCpuLightScheduler(),
                                                        () -> onRejected(scheduler),
                                                        () -> lastRetryTimestamp.set(MIN_VALUE),
                                                        ofMillis(SCHEDULER_BUSY_RETRY_INTERVAL_MS));

      // FlatMap is the way reactor has to do parallel processing. Since this proactor method is used for the processors that are
      // not CPU_LITE, parallelism is wanted when the processor is blocked to do IO or doing long CPU work.
      if (maxConcurrency == 1) {
        // If no concurrency needed, execute directly on the same Flux
        return publisher -> scheduleProcessor(processor, retryScheduler, from(publisher))
            .subscriberContext(ctx -> ctx.put(PROCESSOR_SCHEDULER_CONTEXT_KEY, scheduler));
      } else if (maxConcurrency == MAX_VALUE) {
        if ((processor instanceof InterceptedReactiveProcessor)
            && SDK_OPERATION_CLASS != null
            && SDK_OPERATION_CLASS.isAssignableFrom(((InterceptedReactiveProcessor) processor).getProcessor().getClass())) {
          // For no limit, the java SDK already does a flatMap internally, so no need to do an additional one here
          return publisher -> scheduleProcessor(processor, retryScheduler, from(publisher))
              .subscriberContext(ctx -> ctx.put(PROCESSOR_SCHEDULER_CONTEXT_KEY, scheduler));
        } else {
          // For no limit, pass through the no limit meaning to Reactor's flatMap
          return publisher -> from(publisher)
              .flatMap(event -> scheduleProcessor(processor, retryScheduler, Mono.just(event))
                  .subscriberContext(ctx -> ctx.put(PROCESSOR_SCHEDULER_CONTEXT_KEY, scheduler)),
                       MAX_VALUE);
        }
      } else {
        // Otherwise, enforce the concurrency limit from the config,
        return publisher -> from(publisher)
            .flatMap(event -> scheduleProcessor(processor, retryScheduler, Mono.just(event))
                .subscriberContext(ctx -> ctx.put(PROCESSOR_SCHEDULER_CONTEXT_KEY, scheduler)),
                     max(maxConcurrency / (getParallelism() * subscribers), 1));
      }
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

    private Mono<CoreEvent> scheduleProcessor(ReactiveProcessor processor, ScheduledExecutorService processorScheduler,
                                              Mono<CoreEvent> eventFlux) {
      return scheduleWithLogging(processor, processorScheduler, eventFlux);
    }

    private Flux<CoreEvent> scheduleProcessor(ReactiveProcessor processor, ScheduledExecutorService processorScheduler,
                                              Flux<CoreEvent> eventFlux) {
      return scheduleWithLogging(processor, processorScheduler, eventFlux);
    }

    private Mono<CoreEvent> scheduleWithLogging(ReactiveProcessor processor, ScheduledExecutorService processorScheduler,
                                                Mono<CoreEvent> eventFlux) {
      if (isThreadLoggingEnabled) {
        return Mono.from(eventFlux)
            .flatMap(e -> subscriberContext()
                .flatMap(ctx -> Mono.just(e).transform(processor)
                    .subscribeOn(fromExecutorService(new ThreadLoggingExecutorServiceDecorator(ctx
                        .getOrEmpty(
                                    THREAD_NOTIFICATION_LOGGER_CONTEXT_KEY),
                                                                                               decorateScheduler(
                                                                                                                 processorScheduler),
                                                                                               e.getContext().getId())))));
      } else {
        return Mono.from(eventFlux)
            .publishOn(fromExecutorService(decorateScheduler(processorScheduler)))
            .transform(processor);
      }
    }

    private Flux<CoreEvent> scheduleWithLogging(ReactiveProcessor processor, ScheduledExecutorService processorScheduler,
                                                Flux<CoreEvent> eventFlux) {
      if (isThreadLoggingEnabled) {
        return Flux.from(eventFlux)
            .flatMap(e -> subscriberContext()
                .flatMap(ctx -> Mono.just(e).transform(processor)
                    .subscribeOn(fromExecutorService(new ThreadLoggingExecutorServiceDecorator(ctx
                        .getOrEmpty(
                                    THREAD_NOTIFICATION_LOGGER_CONTEXT_KEY),
                                                                                               decorateScheduler(
                                                                                                                 processorScheduler),
                                                                                               e.getContext().getId())))));
      } else {
        return Flux.from(eventFlux)
            .publishOn(fromExecutorService(decorateScheduler(processorScheduler)))
            .transform(processor);
      }
    }

    @Override
    protected BackPressureReason checkCapacity(CoreEvent event) {
      if (lastRetryTimestamp.get() != MIN_VALUE) {
        if (lastRetryTimestamp.updateAndGet(lastRetryTimestampCheckOperator) != MIN_VALUE) {
          // If there is maxConcurrency value set, honor it and don't buffer here
          if (!maxConcurrencyEagerCheck) {
            // TODO MULE-17265 Make this configurable in the flow
            // This will allow the event to get into the flow, effectively getting into the flow's sink buffer if it cannot be
            // processed right away
            if (queuedEvents.incrementAndGet() > getBufferQueueSize()) {
              queuedEvents.decrementAndGet();
              return REQUIRED_SCHEDULER_BUSY_WITH_FULL_BUFFER;
            }

            // onResponse doesn't wait for child contexts to be terminated, which is handy when a child context is created (like in
            // an async, for instance)
            ((BaseEventContext) event.getContext()).onResponse(queuedDecrementCallback);
          } else {
            return REQUIRED_SCHEDULER_BUSY;
          }
        }
      }

      return super.checkCapacity(event);
    }

    private void onRejected(ScheduledExecutorService scheduler) {
      LOGGER.trace("Shared scheduler {} is busy. Scheduling of the current event will be retried after {}ms.",
                   (scheduler instanceof Scheduler
                       ? ((Scheduler) scheduler).getName()
                       : scheduler.toString()),
                   SCHEDULER_BUSY_RETRY_INTERVAL_MS);
      lastRetryTimestamp.set(nanoTime());
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
