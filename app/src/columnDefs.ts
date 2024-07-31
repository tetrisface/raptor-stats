export default function columnsToColDefs(
  columns: any[],
  dataParam: string,
): any[] {
  const nonPercentFLoat = (params: any) =>
    params.value?.toFixed(1).toString().replace(/\.0+$/, '').padEnd(4, ' ')

  const columnDefs = columns.reduce((acc, column) => {
    acc[column.name] = {
      field: column.name,
      headerName: column.name,
      filter: column.type === 'number' ? 'agNumberColumnFilter' : true,
      cellEditor: ['Copy Paste', 'Winners', 'Players'].includes(column.name)
        ? 'agLargeTextCellEditor'
        : 'agTextCellEditor',
      valueFormatter: (params: any) => {
        if (params.value === '0' || params.value === 0) {
          return ''
        }
      },
    }

    return acc
  }, {})

  if (dataParam.includes('grouped')) {
    Object.entries(columnDefs).forEach(
      ([columnName, columnDef]: [string, any]) => {
        if (columnName.includes('Replays')) {
          columnDefs[columnName] = {
            ...columnDef,
            ...{
              cellEditor: 'agLargeTextCellEditor',
              cellRenderer: 'ReplayLink',
              editable: false,
              filter: false,
            },
          }
        }
        columnDefs[columnName].pinned =
          ['Winners', 'Players', 'Difficulty'].includes(columnName) ||
          columnName.includes('#')
      },
    )
    columnDefs['Difficulty'].width = 103
    columnDefs['Difficulty'].valueFormatter = (params: any) =>
      (params.value * 100).toFixed(2) + '%'
    columnDefs['Winners'].width = 110
    columnDefs['#Winners'].width = 60
    columnDefs['#Winners'].headerName = '#'
    columnDefs['Players'].width = 110
    columnDefs['#Players'].width = 60
    columnDefs['#Players'].headerName = '#'
    columnDefs['Win Replays'].width = 100
    columnDefs['Merged Win Replays'].width = 160
    columnDefs['Loss Replays'].width = 110
    columnDefs['Merged Loss Replays'].width = 160
    columnDefs['Copy Paste'].cellRenderer = 'CellCopy'
    // columnDefs['Copy Paste'].valueFormatter = (params: any) => {
    //   // Trim leading and trailing whitespace from the value
    //   return params.value
    //     ? params.value.replace(/^\s+|\s+$/g, '').replace(/(\r\n|\n|\r)/g, ' ')
    //     : ''
    // }
    if (dataParam.includes('Barbarian')) {
      columnDefs['Barbarian Per Player'].valueFormatter = nonPercentFLoat
    }
  }

  const percentFloat = (params: any) =>
    `${((params?.value ?? 0) * 100).toFixed(0)}%`

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
