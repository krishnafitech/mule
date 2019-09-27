/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.processor.strategy;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.core.api.construct.BackPressureReason;
import org.mule.runtime.core.api.construct.FlowConstruct;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.api.processor.Sink;
import org.mule.runtime.core.internal.processor.strategy.StreamEmitterProcessingStrategyFactory.StreamEmitterProcessingStrategy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class StreamEmitterProcessingStrategyDecorator extends StreamEmitterProcessingStrategy {

  protected final StreamEmitterProcessingStrategy delegate;

  public StreamEmitterProcessingStrategyDecorator(StreamEmitterProcessingStrategy delegate) {
    super(1, 1, () -> null, 1, 1, false);
    this.delegate = delegate;
  }

  @Override
  public Sink createSink(FlowConstruct flowConstruct, ReactiveProcessor function) {
    return delegate.createSink(flowConstruct, function);
  }

  @Override
  public ReactiveProcessor onPipeline(ReactiveProcessor pipeline) {
    return delegate.onPipeline(pipeline);
  }

  @Override
  public Scheduler getFlowDispatcherScheduler() {
    return delegate.getFlowDispatcherScheduler();
  }

  @Override
  public int getBufferQueueSize() {
    return delegate.getBufferQueueSize();
  }

  @Override
  public ReactiveProcessor onProcessor(
                                       ReactiveProcessor processor) {
    return delegate.onProcessor(processor);
  }

  @Override
  public ReactiveProcessor onNonBlockingProcessorTxAware(ReactiveProcessor processor) {
    return delegate.onNonBlockingProcessorTxAware(processor);
  }

  @Override
  public void checkBackpressureAccepting(CoreEvent event) throws RejectedExecutionException {
    delegate.checkBackpressureAccepting(event);
  }

  @Override
  public BackPressureReason checkBackpressureEmitting(
                                                      CoreEvent event) {
    return delegate.checkBackpressureEmitting(event);
  }

  @Override
  public BackPressureReason checkCapacity(CoreEvent event) {
    return delegate.checkCapacity(event);
  }

  @Override
  public int getParallelism() {
    return delegate.getParallelism();
  }

  @Override
  public void start() throws MuleException {
    delegate.start();
  }

  @Override
  public Scheduler createCpuLightScheduler(Supplier<Scheduler> cpuLightSchedulerSupplier) {
    return delegate.createCpuLightScheduler(cpuLightSchedulerSupplier);
  }

  @Override
  public void stop() throws MuleException {
    delegate.stop();
  }

  @Override
  public Scheduler getCpuLightScheduler() {
    return delegate.getCpuLightScheduler();
  }

  @Override
  public void awaitSubscribersCompletion(FlowConstruct flowConstruct,
                                         long shutdownTimeout,
                                         CountDownLatch completionLatch,
                                         long startMillis) {
    delegate.awaitSubscribersCompletion(flowConstruct, shutdownTimeout, completionLatch, startMillis);
  }

  @Override
  public Consumer<CoreEvent> createOnEventConsumer() {
    return delegate.createOnEventConsumer();
  }

  @Override
  public ScheduledExecutorService decorateScheduler(ScheduledExecutorService scheduler) {
    return delegate.decorateScheduler(scheduler);
  }

  @Override
  public boolean isSchedulerBusy(Throwable t) {
    return delegate.isSchedulerBusy(t);
  }

  @Override
  public boolean isSynchronous() {
    return delegate.isSynchronous();
  }
}
