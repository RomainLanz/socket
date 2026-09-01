import { HttpContextFactory } from '@adonisjs/core/factories/http'
import type { HttpContext } from '@adonisjs/core/http'

export function makeHttpContext(): HttpContext {
  return new HttpContextFactory().create()
}
