import { parquetMetadata, parquetRead } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import columnsToColDefs from './columnDefs'
import { FetchParams } from './types'

const JoinUrlParts = (baseUrl: string, ...paths: string[]): string => {
  // Create a new URL object from the base URL
  const url = new URL(baseUrl)

  // Append each path segment to the URL object
  paths.forEach((path) => {
    url.pathname = `${url.pathname.replace(/\/$/, '')}/${path.replace(
      /^\//,
      '',
    )}`
  })

  return url.toString()
}

const Capitalize = (str: string) => {
  return str.charAt(0).toUpperCase() + str.slice(1)
}

export const fetchData = async (params: FetchParams) => {
  const { view, ai, filter, rowData = {}, colDefs = {} } = params
  let _file = 'gamesetting_games.parquet'
  if (view === 'gamesettings') {
    _file = `${Capitalize(ai.toString())}.${filter
      .toString()
      .replace('easy', 'cheese')}.grouped_gamesettings.parquet`
  } else if (view === 'ratings') {
    _file = `PveRating.${Capitalize(ai.toString())}_gamesettings.parquet`
  }

  try {
    const response = await fetch(
      JoinUrlParts(import.meta.env.VITE_FILE_SERVE_HOST, _file),
    )

    const arrayBuffer = await response.arrayBuffer()
    await parquetRead({
      file: arrayBuffer,
      compressors,
      onComplete: (parquetData: any) => {
        const schema = parquetMetadata(arrayBuffer).schema

        const columns = schema.reduce(
          (acc: { [key: string]: string }[], column, index) => {
            ;(schema[index].type !== undefined ||
              column.name == 'Top-5 Difficulties' ||
              column.name.includes('Replays')) &&
              schema[index]?.name !== 'element' &&
              acc.push({
                name: column.name,
                type: column.type === 'BYTE_ARRAY' ? 'string' : 'number',
              })
            return acc
          },
          [],
        )

        rowData.value = parquetData.map((row: any) =>
          row.reduce((acc: any, value: any, index: number) => {
            if (columns[index]) acc[columns[index].name] = value
            return acc
          }, {}),
        )

        colDefs.value = columnsToColDefs(columns, view, ai, filter)
      },
    })
  } catch (error) {
    console.error('Fetch or read error:', error)
  }
}
