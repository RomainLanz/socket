export class MessageQueue<Key = string> {
  #queues = new Map<Key, Promise<void>>()
  #depths = new Map<Key, number>()

  enqueue(key: Key, task: () => Promise<void>, options: { maxDepth?: number } = {}): boolean {
    const depth = this.#depths.get(key) ?? 0

    if (options.maxDepth !== undefined && depth >= options.maxDepth) {
      return false
    }

    this.#depths.set(key, depth + 1)

    const previous = this.#queues.get(key) ?? Promise.resolve()
    const next = previous
      .catch(() => {})
      .then(task)
      .catch(() => {})
      .finally(() => {
        const nextDepth = (this.#depths.get(key) ?? 1) - 1

        if (nextDepth <= 0) {
          this.#depths.delete(key)
        } else {
          this.#depths.set(key, nextDepth)
        }

        if (this.#queues.get(key) === next) {
          this.#queues.delete(key)
        }
      })

    this.#queues.set(key, next)
    return true
  }

  drain(key: Key): Promise<void> {
    return this.#queues.get(key) ?? Promise.resolve()
  }

  delete(key: Key): void {
    this.#queues.delete(key)
    this.#depths.delete(key)
  }

  clear(): void {
    this.#queues.clear()
    this.#depths.clear()
  }
}
