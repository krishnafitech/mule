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

  private StreamEmitterProcessingStrategy delegtate;

  public StreamEmitterProcessingStrategyDecorator(StreamEmitterProcessingStrategy delegate) {
    super(1, 1, () -> null, 1, 1, false);
    this.delegtate = delegate;
  }

  @Override
  public Sink createSink(FlowConstruct flowConstruct, ReactiveProcessor function) {
    return delegtate.createSink(flowConstruct, function);
  }

  @Override
  public ReactiveProcessor onPipeline(ReactiveProcessor pipeline) {
    return delegtate.onPipeline(pipeline);
  }

  @Override
  public Scheduler getFlowDispatcherScheduler() {
    return delegtate.getFlowDispatcherScheduler();
  }

  @Override
  public int getBufferQueueSize() {
    return delegtate.getBufferQueueSize();
  }

  @Override
  public ReactiveProcessor onProcessor(
                                       ReactiveProcessor processor) {
    return delegtate.onProcessor(processor);
  }

  @Override
  public ReactiveProcessor onNonBlockingProcessorTxAware(ReactiveProcessor processor) {
    return delegtate.onNonBlockingProcessorTxAware(processor);
  }

  @Override
  public void checkBackpressureAccepting(CoreEvent event) throws RejectedExecutionException {
    delegtate.checkBackpressureAccepting(event);
  }

  @Override
  public BackPressureReason checkBackpressureEmitting(
                                                      CoreEvent event) {
    return delegtate.checkBackpressureEmitting(event);
  }

  @Override
  public BackPressureReason checkCapacity(CoreEvent event) {
    return delegtate.checkCapacity(event);
  }

  @Override
  public int getParallelism() {
    return delegtate.getParallelism();
  }

  @Override
  public void start() throws MuleException {
    delegtate.start();
  }

  @Override
  public Scheduler createCpuLightScheduler(Supplier<Scheduler> cpuLightSchedulerSupplier) {
    return delegtate.createCpuLightScheduler(cpuLightSchedulerSupplier);
  }

  @Override
  public void stop() throws MuleException {
    delegtate.stop();
  }

  @Override
  public Scheduler getCpuLightScheduler() {
    return delegtate.getCpuLightScheduler();
  }

  @Override
  public void awaitSubscribersCompletion(FlowConstruct flowConstruct,
                                         long shutdownTimeout,
                                         CountDownLatch completionLatch,
                                         long startMillis) {
    delegtate.awaitSubscribersCompletion(flowConstruct, shutdownTimeout, completionLatch, startMillis);
  }

  @Override
  public Consumer<CoreEvent> createOnEventConsumer() {
    return delegtate.createOnEventConsumer();
  }

  @Override
  public ScheduledExecutorService decorateScheduler(ScheduledExecutorService scheduler) {
    return delegtate.decorateScheduler(scheduler);
  }

  @Override
  public boolean isSchedulerBusy(Throwable t) {
    return delegtate.isSchedulerBusy(t);
  }

  @Override
  public boolean isSynchronous() {
    return delegtate.isSynchronous();
  }
}
