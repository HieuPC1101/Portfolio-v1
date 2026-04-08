import type { DashboardData } from "@/types/dashboard";

function formatMockChartLabel(dayOffset: number): string {
  const startDate = new Date(2026, 0, 1);
  const pointDate = new Date(startDate);
  pointDate.setDate(startDate.getDate() + dayOffset);

  const day = String(pointDate.getDate()).padStart(2, "0");
  const month = String(pointDate.getMonth() + 1).padStart(2, "0");

  return `${day}/${month}`;
}

export const dashboardMock: DashboardData = {
  indices: [
    { name: "VN-Index", value: 1285.42, change: 12.35, changePercent: 0.97 },
    { name: "VN30", value: 1312.08, change: 8.74, changePercent: 0.67 },
    { name: "HNX-Index", value: 234.56, change: -1.23, changePercent: -0.52 },
    { name: "UPCOM", value: 92.34, change: 0.45, changePercent: 0.49 },
  ],
  topGainers: [
    { symbol: "MWG", price: 56200, change: 1800, percent: 3.31, liquidity: 175_000_000_000 },
    { symbol: "FPT", price: 132500, change: 3500, percent: 2.71, liquidity: 145_000_000_000 },
    { symbol: "TCB", price: 35700, change: 700, percent: 2.0, liquidity: 132_000_000_000 },
    { symbol: "VCB", price: 85400, change: 1200, percent: 1.43, liquidity: 128_000_000_000 },
  ],
  topLosers: [
    { symbol: "HPG", price: 28350, change: -850, percent: -2.91, liquidity: 160_000_000_000 },
    { symbol: "VNM", price: 72600, change: -400, percent: -0.55, liquidity: 92_000_000_000 },
    { symbol: "PVD", price: 25100, change: -600, percent: -2.33, liquidity: 87_500_000_000 },
    { symbol: "GAS", price: 78100, change: -900, percent: -1.14, liquidity: 64_200_000_000 },
  ],
  topMostActive: [
    { symbol: "HPG", price: 28350, change: -850, percent: -2.91, liquidity: 260_000_000_000 },
    { symbol: "VIX", price: 18100, change: 780, percent: 4.5, liquidity: 210_000_000_000 },
    { symbol: "SHB", price: 12200, change: 360, percent: 3.04, liquidity: 198_000_000_000 },
    { symbol: "SSI", price: 32100, change: -350, percent: -1.08, liquidity: 176_000_000_000 },
  ],
  chart: Array.from({ length: 30 }, (_, i) => ({
    day: i + 1,
    label: formatMockChartLabel(i),
    value: Number((1250 + Math.sin(i * 0.3) * 20 + i * 1.2).toFixed(2)),
  })),
  summary: {
    totalValue: 523_400_000,
    totalInvested: 450_000_000,
    pnl: 73_400_000,
    pnlPercent: 16.31,
    portfolioCount: 3,
    stockCount: 12,
  },
};
