import { context, propagation, trace } from '@opentelemetry/api'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base'

let initialized = false
let exporter: InMemorySpanExporter

export function setupTracing() {
  if (!initialized) {
    exporter = new InMemorySpanExporter()

    context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())
    propagation.setGlobalPropagator(new W3CTraceContextPropagator())
    trace.setGlobalTracerProvider(
      new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] })
    )

    initialized = true
  }

  return { exporter }
}

export function resetSpans() {
  exporter?.reset()
}

export function getFinishedSpans() {
  return exporter?.getFinishedSpans() ?? []
}
