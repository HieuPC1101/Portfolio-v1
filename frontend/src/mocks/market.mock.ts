import type { MarketData } from "@/types/market";

export const marketMock: MarketData = {
  sectors: [
    { name: "Ngân hàng", size: 35, change: 1.2 },
    { name: "Bất động sản", size: 20, change: -0.8 },
    { name: "Công nghệ", size: 15, change: 2.5 },
    { name: "Thép", size: 10, change: -1.5 },
    { name: "Bán lẻ", size: 8, change: 0.9 },
    { name: "Dầu khí", size: 7, change: -0.3 },
    { name: "Thực phẩm", size: 5, change: 0.1 },
  ],
  topGainers: [
    { symbol: "MWG", price: 56200, percent: 3.31 },
    { symbol: "FPT", price: 132500, percent: 2.71 },
    { symbol: "TCB", price: 35700, percent: 2.0 },
    { symbol: "VCB", price: 85400, percent: 1.43 },
  ],
  topLosers: [
    { symbol: "HPG", price: 28350, percent: -2.91 },
    { symbol: "SSI", price: 32100, percent: -2.13 },
    { symbol: "VRE", price: 28900, percent: -1.7 },
    { symbol: "VNM", price: 72600, percent: -0.55 },
  ],
  foreignFlow: [
    { day: "T2", buy: 850, sell: -720 },
    { day: "T3", buy: 920, sell: -1050 },
    { day: "T4", buy: 780, sell: -650 },
    { day: "T5", buy: 1100, sell: -890 },
    { day: "T6", buy: 950, sell: -1020 },
  ],
};
