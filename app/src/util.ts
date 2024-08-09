import { QueryParams } from './types'
const RouteGenerator = (params: QueryParams) => {
  const { view, ai, filter, row } = params
  return `/?view=${view}&ai=${ai}&filter=${filter}&row=${row ?? ''}`
}

export { RouteGenerator }
