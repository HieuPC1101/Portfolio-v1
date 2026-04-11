export interface OptimizeMetricSummary {
  expectedReturn: number;
  volatility: number;
  sharpeRatio: number;
  maxDrawdown?: number;
  cvar?: number;
  cdar?: number;
  beta?: number;
}

export interface OptimizeAlgorithm {
  id: string;
  name: string;
  desc: string;
}

export interface OptimizeWeight {
  symbol: string;
  weight: number;
}

export interface OptimizeAllocation {
  symbol: string;
  weight: number;
  shares: number;
  amount: number;
}

export interface OptimizeBacktestPoint {
  day: number;
  portfolio: number;
  benchmark: number;
}

export interface OptimizeResultData {
  weights: OptimizeWeight[];
  allocation: OptimizeAllocation[];
  metrics: OptimizeMetricSummary;
  backtest: OptimizeBacktestPoint[];
}

export interface OptimizePageData {
  algorithms: OptimizeAlgorithm[];
  result: OptimizeResultData;
}

export interface OptimizeStockSearchItem {
  symbol: string;
  name: string | null;
  exchange: string | null;
  currentPrice?: number;
}

export interface OptimizationConstraints {
  riskFreeRate?: number;
  targetReturn?: number | null;
  maxWeight?: number;
  minWeight?: number;
}

export interface CalculateOptimizationPayload {
  stocks: string[];
  algorithm: string;
  budget: number;
  constraints?: OptimizationConstraints;
  portfolioId?: number;
  startDate?: string;
  endDate?: string;
}
