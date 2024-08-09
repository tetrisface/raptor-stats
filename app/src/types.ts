export type ViewType = 'recent_games' | 'gamesettings' | 'ratings'
export type AIType = 'Barbarian' | 'Raptors' | 'Scavengers'
export type FilterType = 'regular' | 'unbeaten' | 'easy'
export type RowType = number | null | undefined | ''

export type QueryParams = {
  view: ViewType
  ai: AIType
  filter: FilterType
  row: RowType
}

export interface RowData {
  [key: string]: any
}

export interface ColDefs {
  [key: string]: any
}

export interface FetchParams {
  view: ViewType
  ai: AIType
  filter: FilterType
  rowData: RowData
  colDefs: ColDefs
}
export type View = {
  title: any
  view: ViewType
  ai?: AIType
  filter?: FilterType
}
