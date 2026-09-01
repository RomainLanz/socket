type Callable = (...args: never[]) => unknown

export function reportClientError(error: unknown): void {
  try {
    const reportError = (
      globalThis as typeof globalThis & { reportError?: (error: unknown) => void }
    ).reportError

    if (typeof reportError === 'function') {
      reportError(error)
    } else {
      console.error(error)
    }
  } catch {
    // Error reporting must never affect socket state or create another rejected promise.
  }
}

export function callClientHandler(handler: Callable, data?: unknown): void {
  try {
    const result = Reflect.apply(handler, undefined, [data])
    if (result && typeof (result as PromiseLike<unknown>).then === 'function') {
      void Promise.resolve(result).catch(reportClientError)
    }
  } catch (error) {
    reportClientError(error)
  }
}
