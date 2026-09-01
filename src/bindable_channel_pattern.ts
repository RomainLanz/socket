import { ChannelPatternSyntax } from './channel_pattern_syntax.js'

export interface BindableChannelParameter {
  readonly name: string
  readonly optional: boolean
}

type BindableChannelSegment =
  | { readonly kind: 'static'; readonly value: string }
  | {
      readonly kind: 'parameter'
      readonly name: string
      readonly optional: boolean
      readonly wildcard: boolean
    }

function isUnsupportedParameter(
  segment: { readonly name: string; readonly suffix: string; readonly optional: boolean },
  final: boolean,
  parameterNames: ReadonlySet<string>
): boolean {
  return (
    segment.suffix !== '' ||
    !/^[^.:?/*]+$/.test(segment.name) ||
    (segment.optional && !final) ||
    segment.name === 'wildcard' ||
    parameterNames.has(segment.name)
  )
}

/**
 * The deliberately small subset of channel patterns that generated clients can bind.
 * Runtime routing keeps using the complete grammar through ChannelPattern.
 */
export class BindableChannelPattern extends ChannelPatternSyntax {
  readonly parameters: readonly BindableChannelParameter[]
  readonly #bindable: boolean
  readonly #bindingSegments: readonly BindableChannelSegment[]

  private constructor(private readonly source: string) {
    super(source)

    const bindingSegments: BindableChannelSegment[] = []
    const parameters: BindableChannelParameter[] = []
    const parameterNames = new Set<string>()
    const serializedSegments: string[] = []
    let bindable = true

    // Reject syntax that does not map to one complete segment per slash.
    if (
      this.canonicalPattern !== '/' &&
      (this.segments.length === 0 ||
        this.segments.length !== this.canonicalPattern.split('/').length)
    ) {
      bindable = false
    }

    for (const [index, segment] of this.segments.entries()) {
      const final = index === this.segments.length - 1

      if (segment.kind === 'static') {
        if (segment.value.length === 0 || /[:*?]/.test(segment.value)) {
          bindable = false
          break
        }
        bindingSegments.push({ kind: 'static', value: segment.value })
        serializedSegments.push(segment.value)
        continue
      }

      if (segment.kind === 'parameter') {
        // Generated parameters do not support suffixes or intermediate optional values.
        // The wildcard name is reserved for the final wildcard segment.
        if (isUnsupportedParameter(segment, final, parameterNames)) {
          bindable = false
          break
        }

        parameterNames.add(segment.name)
        parameters.push({ name: segment.name, optional: segment.optional })
        bindingSegments.push({
          kind: 'parameter',
          name: segment.name,
          optional: segment.optional,
          wildcard: false,
        })
        serializedSegments.push(`:${segment.name}${segment.optional ? '?' : ''}`)
        continue
      }

      // A generated wildcard must be the final segment and must use the reserved key.
      if (!final || segment.name !== '*' || parameterNames.has('wildcard')) {
        bindable = false
        break
      }

      parameterNames.add('wildcard')
      parameters.push({ name: 'wildcard', optional: false })
      bindingSegments.push({
        kind: 'parameter',
        name: 'wildcard',
        optional: false,
        wildcard: true,
      })
      serializedSegments.push(segment.name)
    }

    // Reject syntax that the parser cannot reproduce without a change.
    if (this.canonicalPattern !== '/' && serializedSegments.join('/') !== this.canonicalPattern) {
      bindable = false
    }

    this.#bindable = bindable
    this.parameters = Object.freeze(parameters.map((parameter) => Object.freeze(parameter)))
    this.#bindingSegments = Object.freeze(bindingSegments.map((segment) => Object.freeze(segment)))
    Object.freeze(this)
  }

  /**
   * Parse and validate the pattern subset that generated clients can bind.
   */
  static parse(value: string): BindableChannelPattern | null {
    const pattern = new BindableChannelPattern(value)
    return pattern.#bindable ? pattern : null
  }

  /**
   * Bind all required parameters and return one concrete channel name.
   */
  bind(parameters: object = {}): string {
    if (this.canonicalPattern === '/') {
      return '/'
    }

    const values: string[] = []
    for (const segment of this.#bindingSegments) {
      if (segment.kind === 'static') {
        values.push(segment.value)
        continue
      }

      const value = Reflect.get(parameters, segment.name) as unknown

      // Omit only an optional parameter. All other segments need one value.
      if (value === undefined && segment.optional) {
        continue
      }
      if (value === undefined) {
        const label = segment.wildcard ? 'wildcard parameter' : `"${segment.name}" parameter`
        throw new Error(`Missing ${label} for channel: ${this.source}`)
      }
      if (typeof value !== 'string' && typeof value !== 'number') {
        throw new Error(`Invalid "${segment.name}" parameter for channel: ${this.source}`)
      }

      const bound = String(value)

      // An ordinary parameter must stay in one segment. A wildcard can contain slashes.
      if (bound.length === 0 || (!segment.wildcard && bound.includes('/'))) {
        throw new Error(`Invalid "${segment.name}" parameter for channel: ${this.source}`)
      }
      values.push(bound)
    }

    return values.join('/') || '/'
  }
}
