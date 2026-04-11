export interface PortfolioHolding {
  id: string;
  symbol: string;
  shares: number;
  avgPrice: number;
  currentPrice: number;
  weight: number;
}

export interface PortfolioItem {
  id: string;
  name: string;
  description?: string;
  totalInvested: number;
  currentValue: number;
  pnl: number;
  pnlPercent: number;
  holdings: PortfolioHolding[];
}

export interface PortfolioListData {
  portfolios: PortfolioItem[];
}

export interface CreatePortfolioPayload {
  name: string;
  description?: string;
  totalInvestment: number;
}

export interface UpdatePortfolioPayload {
  name?: string;
  description?: string;
  totalInvestment?: number;
}

export interface AddStockPayload {
  symbol: string;
  shares: number;
  purchasePrice: number;
}

export interface UpdateStockPayload {
  shares?: number;
  purchasePrice?: number;
}
