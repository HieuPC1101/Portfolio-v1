import type { WatchlistData } from "@/types/watchlist";

export const watchlistMock: WatchlistData = {
  items: [
    { symbol: "VCB", name: "Vietcombank", price: 85400, change: 1200, percent: 1.43 },
    { symbol: "FPT", name: "FPT Corp", price: 132500, change: 3500, percent: 2.71 },
    { symbol: "HPG", name: "Hòa Phát", price: 28350, change: -850, percent: -2.91 },
    { symbol: "VNM", name: "Vinamilk", price: 72600, change: -400, percent: -0.55 },
    { symbol: "MWG", name: "Thế Giới Di Động", price: 56200, change: 1800, percent: 3.31 },
    { symbol: "TCB", name: "Techcombank", price: 35700, change: 700, percent: 2.0 },
    { symbol: "MBB", name: "MB Bank", price: 26200, change: 350, percent: 1.36 },
    { symbol: "SSI", name: "SSI Securities", price: 32100, change: -700, percent: -2.13 },
  ],
};
