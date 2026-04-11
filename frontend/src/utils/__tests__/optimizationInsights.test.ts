import { describe, expect, it } from "vitest";
import type { OptimizeAllocation, OptimizeMetricSummary } from "@/types/optimize";
import type { PortfolioHolding } from "@/types/portfolio";
import {
  buildOptimizationReport,
  buildRebalancePlan,
  summarizeTopWeights,
} from "@/utils/optimizationInsights";

describe("optimizationInsights", () => {
  it("buildRebalancePlan tạo lệnh mua/bán theo chênh lệch cổ phiếu", () => {
    const holdings: PortfolioHolding[] = [
      { id: "1", symbol: "VNM", shares: 100, avgPrice: 70000, currentPrice: 72000, weight: 60 },
      { id: "2", symbol: "FPT", shares: 50, avgPrice: 120000, currentPrice: 125000, weight: 40 },
    ];
    const allocation: OptimizeAllocation[] = [
      { symbol: "VNM", weight: 50, shares: 70, amount: 5040000 },
      { symbol: "FPT", weight: 50, shares: 80, amount: 10000000 },
    ];

    const plan = buildRebalancePlan(holdings, allocation);

    expect(plan.orders).toHaveLength(2);
    expect(plan.orders[0]).toMatchObject({ symbol: "FPT", action: "BUY", deltaShares: 30 });
    expect(plan.orders[1]).toMatchObject({ symbol: "VNM", action: "SELL", deltaShares: -30 });
    expect(plan.summary.buyValue).toBe(3750000);
    expect(plan.summary.sellValue).toBe(2160000);
  });

  it("buildRebalancePlan tạo lệnh bán khi mã hiện tại không còn trong phân bổ", () => {
    const holdings: PortfolioHolding[] = [
      { id: "1", symbol: "VCB", shares: 120, avgPrice: 82000, currentPrice: 85000, weight: 100 },
    ];
    const allocation: OptimizeAllocation[] = [];

    const plan = buildRebalancePlan(holdings, allocation);

    expect(plan.orders).toHaveLength(1);
    expect(plan.orders[0]).toMatchObject({
      symbol: "VCB",
      action: "SELL",
      currentShares: 120,
      targetShares: 0,
      deltaShares: -120,
    });
  });

  it("summarizeTopWeights trả về chuỗi top tỷ trọng", () => {
    const result = summarizeTopWeights([
      { symbol: "VNM", weight: 48.36 },
      { symbol: "FPT", weight: 31.2 },
      { symbol: "VCB", weight: 20.44 },
    ]);

    expect(result).toBe("VNM 48.36%, FPT 31.2%, VCB 20.44%");
  });

  it("buildOptimizationReport tạo báo cáo đầy đủ các phần", () => {
    const metrics: OptimizeMetricSummary = {
      expectedReturn: 11.46,
      volatility: 31.92,
      sharpeRatio: 0.2,
    };
    const report = buildOptimizationReport({
      portfolioName: "Danh mục chính",
      algorithmName: "Tối đa Sharpe Ratio",
      createdAt: "2026-04-10T12:00:00.000Z",
      metrics,
      topWeightsSummary: "VNM 100%, FPT 0%",
      rebalanceOrders: [
        {
          symbol: "VNM",
          action: "SELL",
          currentShares: 157,
          targetShares: 120,
          deltaShares: -37,
          referencePrice: 63700,
          estimatedAmount: 2356900,
        },
      ],
    });

    expect(report).toContain("BAO CAO TOI UU DANH MUC");
    expect(report).toContain("Danh mục chính");
    expect(report).toContain("Tối đa Sharpe Ratio");
    expect(report).toContain("VNM | BAN");
  });
});
