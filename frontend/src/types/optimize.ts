export interface OptimizeMetricSummary {
  expectedReturn: number;
  volatility: number;
  sharpeRatio: number;
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
