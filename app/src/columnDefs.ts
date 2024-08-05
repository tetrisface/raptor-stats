import _ from 'lodash'
import { ColDef, ValueFormatterParams } from 'ag-grid-community'

const percentFloat = (params: any) =>
  `${((params?.value ?? 0) * 100).toFixed(0)}%`

const nonPercentFLoat = (params: any) =>
  params.value?.toFixed(1).toString().replace(/\.0+$/, '').padEnd(4, ' ')

const setField = (
  target: { [key: string]: ColDef },
  keys: string[],
  source: ColDef,
) => {
  for (const key of keys) {
    _.set(target, key, _.merge(_.get(target, key), source))
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

  _.set(
    columnDefs['Award Rate'],
    'headerTooltip',
    'Weight: 0\n - Eco and Damage award summed (1+1) for all games with more than 1 player divided by the count of those games',
  )
  _.set(
    columnDefs['Weighted Award Rate'],
    'headerTooltip',
    'Weight: 1\n - Same as Award Rate but also multiplied by the number of teammates in each game',
  )
  _.set(
    columnDefs['Difficulty Record'],
    'headerTooltip',
    'Weight: ~0.075\n - Highest difficulty won (winners/players)',
  )
  _.set(
    columnDefs['Difficulty Completion'],
    'headerTooltip',
    'Weight: ~0.075\n - The corresponding completion for the maximum value given by difficulty record * difficulty completion for each gamesetting. The completion is the amount of unique teammates in the gamesetting divided by a mapped value for each lobby size. Solo lobby win gives full completion. 16 player lobby wins requires 40 unique teammates. So 40% completion each 16 player win.',
  )
  _.set(
    columnDefs['Difficulty Losers Sum'],
    'headerTooltip',
    'Weight: 0.4\n - Sum of unique players that lost to gamesettings won by the player',
  )
  _.set(
    columnDefs['#Settings'],
    'headerTooltip',
    'Weight: 0.01\n - Unique settings',
  )
  _.set(
    columnDefs['#Games'],
    'headerTooltip',
    'Weight: 0.4\n - Count of games played from 0 to 20',
  )
  _.set(columnDefs['Win Rate'], 'headerTooltip', 'Weight: 0.005\n - Wins/Games')
  _.set(
    columnDefs['Difficulty Rank'],
    'headerTooltip',
    '(Difficulty Record * Difficulty Completion) ranked',
  )
  _.set(
    columnDefs['Combined Rank'],
    'headerTooltip',
    'Sum of ranks multplied by their weights',
  )
  _.set(
    columnDefs['PVE Rating'],
    'headerTooltip',
    'Linear interpolation of Combined Rank',
  )

  if (dataParam.includes('grouped') || dataParam == 'gamesetting_games') {
    setField(
      columnDefs,
      [
        'Win Replays',
        'Merged Win Replays',
        'Loss Replays',
        'Merged Loss Replays',
      ],
      {
        cellEditor: 'agLargeTextCellEditor',
        cellRenderer: 'ReplayLink',
        editable: false,
        filter: false,
      },
    )

    _.set(columnDefs['AI'], 'width', 93)
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

    _.set(columnDefs['Start Time'], 'width', 192)
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
        width: 61,
        headerName: '#',
      })

      setField(
        columnDefs,
        ['Winners', '#Winners', '#Players', 'Players', 'Difficulty'],
        {
          pinned: true,
        },
      )

      _.set(
        columnDefs['#Games'],
        'headerTooltip',
        'Weight: 0.4\n - Count of games played from 0 to 20',
      )
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
  } else if (dataParam.includes('Rating')) {
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
