import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
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
  ticker?: string | null;
  symbol?: string | null;
  price?: number | null;
  daily_change?: number | null;
  change_percent?: number | null;
  percent?: number | null;
  avg_trading_value_20d?: number | null;
}

function isActiveMover(stock: BackendMover): boolean {
  return typeof stock.price === "number" && Number.isFinite(stock.price) && stock.price > 0;
}

interface BackendTopMovers {
  gainers?: BackendMover[];
  losers?: BackendMover[];
  most_active?: BackendMover[];
}

const MAX_TOP_MOVERS = 10;

function parseLiquidity(value: number | null | undefined): number | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return null;
  }

  return value;
}

function mapMover(stock: BackendMover): TopMover {
  const percentRaw = stock.daily_change ?? stock.change_percent ?? stock.percent;
  const percent = typeof percentRaw === "number" && Number.isFinite(percentRaw) ? percentRaw : 0;
  const symbol = (stock.ticker ?? stock.symbol ?? "").trim().toUpperCase();
  const price = typeof stock.price === "number" && Number.isFinite(stock.price) ? stock.price : 0;

  return {
    symbol,
    price,
    change: parseFloat(((price * percent) / 100).toFixed(0)),
    percent,
    liquidity: parseLiquidity(stock.avg_trading_value_20d),
  };
}

function mapMovers(stocks: BackendMover[] | undefined): TopMover[] {
  if (!Array.isArray(stocks) || stocks.length === 0) {
    return [];
  }

  return stocks
    .filter(isActiveMover)
    .map(mapMover)
    .filter((stock) => stock.symbol.length > 0);
}

function capMovers(stocks: TopMover[]): TopMover[] {
  return stocks.slice(0, MAX_TOP_MOVERS);
}

function resolveTopMovers(
  topGainers: TopMover[],
  topLosers: TopMover[],
  topMostActive: TopMover[],
): { topGainers: TopMover[]; topLosers: TopMover[]; topMostActive: TopMover[] } {
  const mergedBySymbol = new Map<string, TopMover>();

  for (const stock of [...topGainers, ...topLosers, ...topMostActive]) {
    if (!mergedBySymbol.has(stock.symbol)) {
      mergedBySymbol.set(stock.symbol, stock);
    }
  }

  const merged = [...mergedBySymbol.values()];
  const fallbackGainers = [...merged].sort((a, b) => b.percent - a.percent);
  const fallbackLosers = [...merged].sort((a, b) => a.percent - b.percent);
  const fallbackMostActive = [...merged].sort((a, b) => (b.liquidity ?? 0) - (a.liquidity ?? 0));

  const resolvedGainers = topGainers.length > 0 ? topGainers : fallbackGainers;
  const resolvedLosers = topLosers.length > 0 ? topLosers : fallbackLosers;
  const resolvedMostActive = topMostActive.length > 0 ? topMostActive : fallbackMostActive;

  if (resolvedGainers.length > 0 || resolvedLosers.length > 0 || resolvedMostActive.length > 0) {
    return {
      topGainers: capMovers(resolvedGainers),
      topLosers: capMovers(resolvedLosers),
      topMostActive: capMovers(resolvedMostActive),
    };
  }

  return {
    topGainers: capMovers(dashboardMock.topGainers),
    topLosers: capMovers(dashboardMock.topLosers),
    topMostActive: capMovers(dashboardMock.topMostActive),
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
    apiGet<BackendTopMovers>("/api/v1/market/top-movers?top_n=10"),
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

  const mappedGainers = mapMovers(moversRaw.gainers);
  const mappedLosers = mapMovers(moversRaw.losers);
  const mappedMostActive = mapMovers(moversRaw.most_active);
  const {
    topGainers,
    topLosers,
    topMostActive,
  } = resolveTopMovers(mappedGainers, mappedLosers, mappedMostActive);

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
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return dashboardMock;
  }

  return getLiveDashboardData();
}
