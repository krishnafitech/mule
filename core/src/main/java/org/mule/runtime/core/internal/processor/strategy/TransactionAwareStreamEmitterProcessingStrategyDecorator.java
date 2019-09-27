/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.processor.strategy;

import static org.mule.runtime.core.api.transaction.TransactionCoordination.isTransactionActive;
import static org.mule.runtime.core.internal.processor.strategy.BlockingProcessingStrategyFactory.BLOCKING_PROCESSING_STRATEGY_INSTANCE;
import static org.mule.runtime.core.internal.processor.strategy.TransactionAwareProcessingStrategyFactory.LAZY_TX_CHECK;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Mono.just;

import org.mule.runtime.core.api.construct.FlowConstruct;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.api.processor.Sink;
import org.mule.runtime.core.internal.processor.strategy.StreamEmitterProcessingStrategyFactory.StreamEmitterProcessingStrategy;
import org.mule.runtime.core.internal.util.rx.ConditionalExecutorServiceDecorator;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

public class TransactionAwareStreamEmitterProcessingStrategyDecorator extends StreamEmitterProcessingStrategyDecorator {

  public TransactionAwareStreamEmitterProcessingStrategyDecorator(StreamEmitterProcessingStrategy delegate) {
    super(delegate);
  }

  @Override
  public Sink createSink(FlowConstruct flowConstruct, ReactiveProcessor pipeline) {
    Sink proactorSink = super.createSink(flowConstruct, pipeline);
    Sink syncSink = new StreamPerThreadSink(pipeline, createOnEventConsumer(), flowConstruct);
    return new TransactionalDelegateSink(syncSink, proactorSink);
  }

  @Override
  public Consumer<CoreEvent> createOnEventConsumer() {
    // Do nothing given event should still be processed when transaction is active
    return event -> {
    };
  }

  @Override
  public ReactiveProcessor onNonBlockingProcessorTxAware(ReactiveProcessor processor) {
    if (LAZY_TX_CHECK) {
      // If there is a tx active, force the processing to the main thread right after the processor.
      // This is needed because non blocking processors will do a thread switch internally regardless of the presence of a tx
      // (which is ok, an http:reqeust shouldn't bother that a previous db component opened a transaction).

      return publisher -> from(publisher)
          .flatMap(event -> just(event)
              .transform(isTransactionActive() ? BLOCKING_PROCESSING_STRATEGY_INSTANCE.onProcessor(processor) : processor));
    } else {
      return super.onNonBlockingProcessorTxAware(processor);
    }

  }

  @Override
  public ScheduledExecutorService decorateScheduler(ScheduledExecutorService scheduler) {
    return new ConditionalExecutorServiceDecorator(super.decorateScheduler(scheduler),
                                                   currentScheduler -> isTransactionActive());
  }

  @Override
  public ReactiveProcessor onPipeline(ReactiveProcessor pipeline) {
    return !LAZY_TX_CHECK && isTransactionActive() ? BLOCKING_PROCESSING_STRATEGY_INSTANCE.onPipeline(pipeline)
        : super.onPipeline(pipeline);
  }

  @Override
  public ReactiveProcessor onProcessor(ReactiveProcessor processor) {
    return !LAZY_TX_CHECK && isTransactionActive() ? BLOCKING_PROCESSING_STRATEGY_INSTANCE.onProcessor(processor)
        : super.onProcessor(processor);
  }
}
