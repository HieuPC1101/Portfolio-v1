import { isMockMode, MOCK_DELAY_MS } from "@/config/env";
import { apiGet } from "@/lib/api";
import { apiAuthGet } from "@/lib/apiAuth";
import { dashboardMock } from "@/mocks/dashboard.mock";
import type {
  DashboardChartPoint,
  DashboardData,
  MarketIndex,
  PortfolioSummary,
  TopMover,
} from "@/types/dashboard";

interface BackendIndexValue {
  name: string;
  value: number;
  change: number;
  change_percent: number;
}

interface BackendIndices {
  vnindex: BackendIndexValue;
  vn30: BackendIndexValue;
  hnx: BackendIndexValue;
  upcom: BackendIndexValue;
}

interface BackendHistoryPoint {
  time: string;
  close: number;
  symbol: string;
}

interface BackendMover {
  ticker: string;
  price: number;
  daily_change: number;
  avg_trading_value_20d?: number | null;
}

function isActiveMover(stock: BackendMover): boolean {
  return Number.isFinite(stock.price) && stock.price > 0;
}

interface BackendTopMovers {
  gainers: BackendMover[];
  losers: BackendMover[];
  most_active: BackendMover[];
}

function parseLiquidity(value: number | null | undefined): number | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return null;
  }

  return value;
}

function mapMover(stock: BackendMover): TopMover {
  const percent = Number.isFinite(stock.daily_change) ? stock.daily_change : 0;

  return {
    symbol: stock.ticker,
    price: stock.price,
    change: parseFloat(((stock.price * percent) / 100).toFixed(0)),
    percent,
    liquidity: parseLiquidity(stock.avg_trading_value_20d),
  };
}

interface BackendPortfolio {
  id: number;
  total_investment: number;
  stocks: Array<{ id: number }>;
}

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function isVnIndexSymbol(symbol: string): boolean {
  return symbol.replace(/[^A-Za-z]/g, "").toUpperCase() === "VNINDEX";
}

function formatDayMonthLabel(time: string, fallbackIndex: number): string {
  const date = new Date(time);

  if (Number.isNaN(date.getTime())) {
    return `T${fallbackIndex + 1}`;
  }

  const day = String(date.getDate()).padStart(2, "0");
  const month = String(date.getMonth() + 1).padStart(2, "0");

  return `${day}/${month}`;
}

async function getLiveDashboardData(): Promise<DashboardData> {
  const [indicesRaw, historyRaw, moversRaw, portfoliosRaw] = await Promise.all([
    apiGet<BackendIndices>("/api/v1/market/indices"),
    apiGet<BackendHistoryPoint[]>("/api/v1/market/indices/history?symbols=VNINDEX&months=1"),
    apiGet<BackendTopMovers>("/api/v1/market/top-movers?top_n=6"),
    apiAuthGet<BackendPortfolio[]>("/api/v1/portfolios").catch(() => [] as BackendPortfolio[]),
  ]);

  const indices: MarketIndex[] = [
    {
      name: "VN-Index",
      value: indicesRaw.vnindex.value,
      change: indicesRaw.vnindex.change,
      changePercent: indicesRaw.vnindex.change_percent,
    },
    {
      name: "VN30",
      value: indicesRaw.vn30.value,
      change: indicesRaw.vn30.change,
      changePercent: indicesRaw.vn30.change_percent,
    },
    {
      name: "HNX-Index",
      value: indicesRaw.hnx.value,
      change: indicesRaw.hnx.change,
      changePercent: indicesRaw.hnx.change_percent,
    },
    {
      name: "UPCOM",
      value: indicesRaw.upcom.value,
      change: indicesRaw.upcom.change,
      changePercent: indicesRaw.upcom.change_percent,
    },
  ];

  const chart: DashboardChartPoint[] = historyRaw
    .filter((point) => isVnIndexSymbol(point.symbol))
    .sort((a, b) => a.time.localeCompare(b.time))
    .map((point, index) => ({
      day: index + 1,
      label: formatDayMonthLabel(point.time, index),
      value: point.close,
    }));

  const topGainers: TopMover[] = (moversRaw.gainers ?? [])
    .filter(isActiveMover)
    .map(mapMover);

  const topLosers: TopMover[] = (moversRaw.losers ?? [])
    .filter(isActiveMover)
    .map(mapMover);

  const topMostActive: TopMover[] = (moversRaw.most_active ?? [])
    .filter(isActiveMover)
    .map(mapMover);

  const totalInvested = portfoliosRaw.reduce((sum, portfolio) => sum + Number(portfolio.total_investment), 0);
  const summary: PortfolioSummary = {
    totalValue: totalInvested,
    totalInvested,
    pnl: 0,
    pnlPercent: 0,
    portfolioCount: portfoliosRaw.length,
    stockCount: portfoliosRaw.reduce((sum, portfolio) => sum + portfolio.stocks.length, 0),
  };

  return { indices, topGainers, topLosers, topMostActive, chart, summary };
}

export async function getDashboardData(): Promise<DashboardData> {
  if (isMockMode) {
    await delay(MOCK_DELAY_MS);
    return dashboardMock;
  }

  return getLiveDashboardData();
}
