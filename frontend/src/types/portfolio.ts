export interface PortfolioHolding {
  symbol: string;
  shares: number;
  avgPrice: number;
  currentPrice: number;
  weight: number;
}

export interface PortfolioItem {
  id: string;
  name: string;
  totalInvested: number;
  currentValue: number;
  pnl: number;
  pnlPercent: number;
  holdings: PortfolioHolding[];
}

export interface PortfolioListData {
  portfolios: PortfolioItem[];
}
