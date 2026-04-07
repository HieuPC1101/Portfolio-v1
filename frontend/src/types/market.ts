export interface Sector {
  name: string;
  size: number;
  change: number;
}

export interface StockBasic {
  symbol: string;
  price: number;
  percent: number;
}

export interface ForeignFlowPoint {
  day: string;
  buy: number;
  sell: number;
}

export interface MarketData {
  sectors: Sector[];
  topGainers: StockBasic[];
  topLosers: StockBasic[];
  foreignFlow: ForeignFlowPoint[];
}
