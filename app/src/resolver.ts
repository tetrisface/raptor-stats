import { parquetMetadata, parquetRead } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import columnsToColDefs from './columnDefs'

export const fetchData = async (
  dataParam: string,
  rowData: any,
  colDefs: any,
) => {
  try {
    const response = await fetch(
      `${import.meta.env.VITE_FILE_SERVE_HOST}/${dataParam}.parquet`,
    )
    response.ok || console.log('response.ok', response.ok, 'response', response)

    const arrayBuffer = await response.arrayBuffer()
    await parquetRead({
      file: arrayBuffer,
      compressors,
      onComplete: (parquetData: any) => {
        const schema = parquetMetadata(arrayBuffer).schema
        const columns = schema.reduce(
          (acc: { [key: string]: string }[], column, index) => {
            index > 0 &&
              schema[index].name !== 'item' &&
              schema[index].name !== 'list' &&
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
            acc[columns[index].name] = value
            return acc
          }, {}),
        )

        colDefs.value = columnsToColDefs(columns, dataParam)
      },
    })
  } catch (error) {
    console.error('Fetch or read error:', error)
  }
}
