import type { PortfolioListData } from "@/types/portfolio";

export const portfolioMock: PortfolioListData = {
  portfolios: [
    {
      id: "1",
      name: "Danh mục chính",
      totalInvested: 300_000_000,
      currentValue: 345_600_000,
      pnl: 45_600_000,
      pnlPercent: 15.2,
      holdings: [
        { symbol: "VCB", shares: 500, avgPrice: 82000, currentPrice: 85400, weight: 32 },
        { symbol: "FPT", shares: 200, avgPrice: 120000, currentPrice: 132500, weight: 25 },
        { symbol: "VNM", shares: 400, avgPrice: 74000, currentPrice: 72600, weight: 18 },
        { symbol: "HPG", shares: 800, avgPrice: 30000, currentPrice: 28350, weight: 15 },
        { symbol: "MWG", shares: 300, avgPrice: 52000, currentPrice: 56200, weight: 10 },
      ],
    },
    {
      id: "2",
      name: "Danh mục tăng trưởng",
      totalInvested: 150_000_000,
      currentValue: 177_800_000,
      pnl: 27_800_000,
      pnlPercent: 18.53,
      holdings: [
        { symbol: "FPT", shares: 300, avgPrice: 118000, currentPrice: 132500, weight: 45 },
        { symbol: "TCB", shares: 600, avgPrice: 33000, currentPrice: 35700, weight: 30 },
        { symbol: "MBB", shares: 500, avgPrice: 24000, currentPrice: 26200, weight: 25 },
      ],
    },
  ],
};
