export interface WatchlistItem {
  symbol: string;
  name: string;
  price: number;
  change: number;
  percent: number;
}

export interface WatchlistData {
  items: WatchlistItem[];
}

export interface WatchlistMutationPayload {
  symbol: string;
}
