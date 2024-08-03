import _ from 'lodash'
import { ColDef, ValueFormatterParams } from 'ag-grid-community'

const percentFloat = (params: any) =>
  `${((params?.value ?? 0) * 100).toFixed(0)}%`

const nonPercentFLoat = (params: any) =>
  params.value?.toFixed(1).toString().replace(/\.0+$/, '').padEnd(4, ' ')

const setField = (
  target: { [key: string]: ColDef },
  paths: string[],
  source: ColDef,
) => {
  for (const path of paths) {
    _.set(target, path, _.merge(_.get(target, path), source))
  }
  return target
}

export default function columnsToColDefs(
  columns: any[],
  dataParam: string,
): any[] {
  const columnDefs = columns.reduce(
    (acc: { [key: string]: ColDef }, column) => {
      acc[column.name] = {
        field: column.name,
        headerName: column.name,
        filter: column.type === 'number' ? 'agNumberColumnFilter' : true,
        cellEditor: 'agTextCellEditor',
        valueFormatter: (params: ValueFormatterParams) => {
          if (params.value === '0' || params.value === 0) {
            return ''
          }
          return params.value
        },
      }
      return acc
    },
    {},
  )

  if (dataParam.includes('grouped') || dataParam == 'gamesetting_games') {
    Object.entries(columnDefs).forEach(
      ([columnName, columnDef]: [string, any]) => {
        if (columnName.includes('Replays')) {
          _.merge(columnDef, {
            cellEditor: 'agLargeTextCellEditor',
            cellRenderer: 'ReplayLink',
            editable: false,
            filter: false,
          })
        }
        if (dataParam.includes('grouped')) {
          _.set(
            columnDef,
            'pinned',
            ['Winners', 'Players', 'Difficulty'].includes(columnName) ||
              columnName.includes('#'),
          )
        }
      },
    )
    _.set(columnDefs['AI'], 'width', 90)
    _.set(columnDefs['Result'], 'width', 100)
    _.set(columnDefs['Map'], 'width', 170)
    setField(columnDefs, ['#Winners', '#Players'], {
      width: 110,
    })

    _.set(columnDefs['Copy Paste'], 'cellEditor', 'agLargeTextCellEditor')
    setField(columnDefs, ['Winners', 'Players'], {
      cellEditor: 'agLargeTextCellEditor',
      width: 120,
    })

    _.set(columnDefs['Difficulty'], 'width', 103)
    _.set(columnDefs['Difficulty'], 'valueFormatter', (params: any) =>
      params.value ? (params.value * 100).toFixed(2) + '%' : undefined,
    )

    setField(columnDefs, ['Date', 'startTime'], {
      width: 192,
    })
    _.set(columnDefs['Winners'], 'width', 110)
    _.set(columnDefs['Players'], 'width', 110)
    _.set(columnDefs['Win Replays'], 'width', 100)
    _.set(columnDefs['Merged Win Replays'], 'width', 160)
    _.set(columnDefs['Loss Replays'], 'width', 110)
    _.set(columnDefs['Merged Loss Replays'], 'width', 160)
    _.set(columnDefs['Copy Paste'], 'cellRenderer', 'CellCopy')

    _.set(columnDefs['Barbarian Handicap'], 'width', 160)
    setField(columnDefs, ['Barbarian Per Player'], {
      valueFormatter: nonPercentFLoat,
      width: 180,
    })

    if (dataParam.includes('gamesetting_games')) {
      for (const value of Object.values(columnDefs)) {
        value && _.set(value, 'pinned', false)
      }
    }

    if (dataParam.includes('grouped')) {
      setField(columnDefs, ['#Winners', '#Players'], {
        width: 60,
        headerName: '#',
      })
    }
  }

  if (dataParam.includes('Rating')) {
    columnDefs['Player'].pinned = true
    columnDefs['Award Rate'].valueFormatter = percentFloat
    columnDefs['Difficulty Record'].valueFormatter = percentFloat
    columnDefs['Difficulty Completion'].valueFormatter = percentFloat
    columnDefs['Win Rate'].valueFormatter = percentFloat
    columnDefs['Weighted Award Rate'].valueFormatter = nonPercentFLoat
  }

  let columnsArray = Object.values(columnDefs)
  if (dataParam.includes('grouped')) {
    moveItemInArray(columnsArray, '#Winners', 3)
    moveItemInArray(columnsArray, '#Players', 4)
  } else {
    moveItemInArray(columnsArray, 'Combined Rank', 1)
    moveItemInArray(columnsArray, 'PVE Rating', 2)
  }
  return columnsArray
}

const moveItemInArray = (arr: any[], field: string, toIndex: number) => {
  // Ensure the indices are within bounds
  const fromIndex = arr.findIndex((item: any) => item.field === field)
  if (
    fromIndex < 0 ||
    fromIndex >= arr.length ||
    toIndex < 0 ||
    toIndex >= arr.length
  ) {
    console.error('Indices are out of bounds')
    return
  }

  // Remove the item from the original position
  const item = arr.splice(fromIndex, 1)[0]

  // Insert the item at the new position
  arr.splice(toIndex, 0, item)
}
