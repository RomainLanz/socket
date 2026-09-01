import type { AuthenticatedSocket } from './types.js'

type DecoratedChannelHandler<User> = (socket: AuthenticatedSocket<User>, data: unknown) => unknown

type ChannelMessageHandlerDefinition = {
  event: string
  methodName: string | symbol
}

const decoratedHandlers = new WeakMap<object, ChannelMessageHandlerDefinition[]>()

/**
 * Method decorator used to bind an incoming channel event to a method.
 */
export function onMessage<EventName extends string>(event: EventName): MethodDecorator {
  return (target, propertyKey, descriptor) => {
    if (typeof descriptor?.value !== 'function') {
      throw new Error(`@onMessage("${event}") can only be used on methods`)
    }

    const constructor = target.constructor
    const handlers = [...(decoratedHandlers.get(constructor) ?? [])]

    handlers.push({
      event,
      methodName: propertyKey,
    })

    decoratedHandlers.set(constructor, handlers)
  }
}

/**
 * Resolve decorated handlers for a channel instance, including inherited ones.
 * Child classes override parent handlers when the event name is the same.
 */
export function getDecoratedChannelHandlers<User = unknown>(
  instance: object
): Record<string, DecoratedChannelHandler<User>> {
  const constructors: object[] = []

  let proto = Object.getPrototypeOf(instance)
  while (proto && proto !== Object.prototype) {
    const constructor = Reflect.get(proto, 'constructor')
    if (typeof constructor === 'function') {
      constructors.unshift(constructor)
    }
    proto = Object.getPrototypeOf(proto)
  }

  const handlers: Record<string, unknown> = {}

  for (const constructor of constructors) {
    const definitions = decoratedHandlers.get(constructor) ?? []

    for (const definition of definitions) {
      const handler = Reflect.get(instance, definition.methodName)
      if (typeof handler !== 'function') {
        continue
      }

      handlers[definition.event] = handler.bind(instance)
    }
  }

  return handlers as Record<string, DecoratedChannelHandler<User>>
}
