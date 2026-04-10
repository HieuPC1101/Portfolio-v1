import type { OptimizeAllocation, OptimizeMetricSummary, OptimizeWeight } from "@/types/optimize";
import type { PortfolioHolding } from "@/types/portfolio";

export type RebalanceAction = "BUY" | "SELL";

export interface RebalanceOrder {
  symbol: string;
  action: RebalanceAction;
  currentShares: number;
  targetShares: number;
  deltaShares: number;
  referencePrice: number;
  estimatedAmount: number;
}

export interface RebalanceSummary {
  buyValue: number;
  sellValue: number;
  netCashflow: number;
}

export interface RebalancePlan {
  orders: RebalanceOrder[];
  summary: RebalanceSummary;
}

function round(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function estimateReferencePrice(holding: PortfolioHolding | undefined, allocationItem: OptimizeAllocation): number {
  if (holding?.currentPrice && Number.isFinite(holding.currentPrice) && holding.currentPrice > 0) {
    return holding.currentPrice;
  }

  if (allocationItem.shares > 0 && allocationItem.amount > 0) {
    return allocationItem.amount / allocationItem.shares;
  }

  return 0;
}

export function buildRebalancePlan(holdings: PortfolioHolding[], allocation: OptimizeAllocation[]): RebalancePlan {
  const holdingMap = new Map<string, PortfolioHolding>();
  for (const item of holdings) {
    holdingMap.set(item.symbol.toUpperCase(), item);
  }
  const targetSymbols = new Set<string>();

  const orders: RebalanceOrder[] = [];

  for (const target of allocation) {
    const symbol = target.symbol.toUpperCase();
    targetSymbols.add(symbol);
    const current = holdingMap.get(symbol);
    const currentShares = current?.shares ?? 0;
    const targetShares = target.shares;
    const deltaShares = targetShares - currentShares;

    if (deltaShares === 0) {
      continue;
    }

    const referencePrice = estimateReferencePrice(current, target);
    const estimatedAmount = Math.round(Math.abs(deltaShares) * referencePrice);

    orders.push({
      symbol,
      action: deltaShares > 0 ? "BUY" : "SELL",
      currentShares,
      targetShares,
      deltaShares,
      referencePrice: round(referencePrice, 2),
      estimatedAmount,
    });
  }

  for (const holding of holdings) {
    const symbol = holding.symbol.toUpperCase();
    if (targetSymbols.has(symbol) || holding.shares <= 0) {
      continue;
    }

    const referencePrice = holding.currentPrice > 0 ? holding.currentPrice : holding.avgPrice;
    const estimatedAmount = Math.round(holding.shares * referencePrice);

    orders.push({
      symbol,
      action: "SELL",
      currentShares: holding.shares,
      targetShares: 0,
      deltaShares: -holding.shares,
      referencePrice: round(referencePrice, 2),
      estimatedAmount,
    });
  }

  orders.sort((left, right) => right.estimatedAmount - left.estimatedAmount);

  let buyValue = 0;
  let sellValue = 0;
  for (const order of orders) {
    if (order.action === "BUY") {
      buyValue += order.estimatedAmount;
    } else {
      sellValue += order.estimatedAmount;
    }
  }

  return {
    orders,
    summary: {
      buyValue,
      sellValue,
      netCashflow: buyValue - sellValue,
    },
  };
}

export function summarizeTopWeights(weights: OptimizeWeight[], top = 3): string {
  return weights
    .slice(0, top)
    .map((item) => `${item.symbol} ${item.weight}%`)
    .join(", ");
}

function toActionLabel(action: RebalanceAction): "MUA" | "BAN" {
  return action === "BUY" ? "MUA" : "BAN";
}

export interface OptimizationReportInput {
  portfolioName: string;
  algorithmName: string;
  createdAt: string;
  metrics: OptimizeMetricSummary;
  topWeightsSummary: string;
  rebalanceOrders: RebalanceOrder[];
}

export function buildOptimizationReport(input: OptimizationReportInput): string {
  const header = [
    "BAO CAO TOI UU DANH MUC",
    `Thoi gian: ${new Date(input.createdAt).toLocaleString("vi-VN")}`,
    `Danh muc: ${input.portfolioName}`,
    `Thuat toan: ${input.algorithmName}`,
    "",
    "Chi so hieu suat:",
    `- Expected Return: ${input.metrics.expectedReturn}%`,
    `- Volatility: ${input.metrics.volatility}%`,
    `- Sharpe Ratio: ${input.metrics.sharpeRatio}`,
    "",
    `Top ty trong: ${input.topWeightsSummary}`,
    "",
    "Lenh rebalance:",
  ];

  const orders = input.rebalanceOrders.length > 0
    ? input.rebalanceOrders.map((order) => (
      `${order.symbol} | ${toActionLabel(order.action)} | Hien tai: ${order.currentShares} | Muc tieu: ${order.targetShares} | Delta: ${order.deltaShares}`
    ))
    : ["Khong co lenh can thuc hien"];

  return [...header, ...orders].join("\n");
}
