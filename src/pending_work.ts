/**
 * Counts work in flight and settles a promise once none is left. It keeps
 * no reference to the work, so work that never settles retains nothing.
 */
export class PendingWork {
  #count = 0
  #idle = Promise.withResolvers<void>()

  track(work: Promise<unknown>): void {
    if (this.#count++ === 0) this.#idle = Promise.withResolvers()
    const done = () => {
      if (--this.#count === 0) this.#idle.resolve()
    }
    work.then(done, done)
  }

  /** Settles once no tracked work is running, or returns null when none is. */
  settled(): Promise<void> | null {
    return this.#count === 0 ? null : this.#idle.promise
  }
}
