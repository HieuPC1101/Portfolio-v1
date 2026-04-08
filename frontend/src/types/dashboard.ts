export interface MarketIndex {
  name: string;
  value: number;
  change: number;
  changePercent: number;
}

export interface TopMover {
  symbol: string;
  price: number;
  change: number;
  percent: number;
  liquidity: number | null;
}

export interface DashboardChartPoint {
  day: number;
  label: string;
  value: number;
}

export interface PortfolioSummary {
  totalValue: number;
  totalInvested: number;
  pnl: number;
  pnlPercent: number;
  portfolioCount: number;
  stockCount: number;
}

export interface DashboardData {
  indices: MarketIndex[];
  topGainers: TopMover[];
  topLosers: TopMover[];
  topMostActive: TopMover[];
  chart: DashboardChartPoint[];
  summary: PortfolioSummary;
}
