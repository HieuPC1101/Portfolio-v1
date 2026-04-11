import type { OptimizePageData } from "@/types/optimize";

export const optimizeMock: OptimizePageData = {
  algorithms: [
    { id: "markowitz", name: "Markowitz", desc: "Lý thuyết danh mục hiện đại" },
    { id: "max_sharpe", name: "Max Sharpe", desc: "Tối đa hệ số Sharpe" },
    { id: "min_vol", name: "Min Volatility", desc: "Tối thiểu biến động" },
    { id: "hrp", name: "HRP", desc: "Phân cấp rủi ro đồng đều" },
    { id: "min_cvar", name: "Min CVaR", desc: "Tối thiểu rủi ro đuôi" },
    { id: "min_cdar", name: "Min CDaR", desc: "Tối thiểu drawdown có điều kiện" },
  ],
  result: {
    weights: [
      { symbol: "VCB", weight: 32 },
      { symbol: "FPT", weight: 25 },
      { symbol: "VNM", weight: 18 },
      { symbol: "HPG", weight: 15 },
      { symbol: "MWG", weight: 10 },
    ],
    metrics: {
      expectedReturn: 18.5,
      volatility: 12.3,
      sharpeRatio: 1.5,
    },
    allocation: [
      { symbol: "VCB", weight: 32, shares: 450, amount: 32_000_000 },
      { symbol: "FPT", weight: 25, shares: 190, amount: 25_000_000 },
      { symbol: "VNM", weight: 18, shares: 250, amount: 18_000_000 },
      { symbol: "HPG", weight: 15, shares: 530, amount: 15_000_000 },
      { symbol: "MWG", weight: 10, shares: 180, amount: 10_000_000 },
    ],
    backtest: Array.from({ length: 60 }, (_, i) => ({
      day: i + 1,
      portfolio: Number((100 + i * 0.5 + Math.sin(i * 0.2) * 5).toFixed(2)),
      benchmark: Number((100 + i * 0.3 + Math.sin(i * 0.15) * 3).toFixed(2)),
    })),
  },
};
