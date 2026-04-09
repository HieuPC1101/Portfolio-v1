import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiGet } from "@/lib/api";
import { marketMock } from "@/mocks/market.mock";
import type { MarketData, Sector, StockBasic } from "@/types/market";

export interface StockSearchResult {
  symbol: string;
  name: string | null;
  exchange: string | null;
  sector: string | null;
  price: number | null;
  percent: number | null;
}

export interface StockPricePoint {
  date: string;
  close: number;
}

export interface OHLCPoint {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface StockDetailData {
  symbol: string;
  name: string | null;
  exchange: string | null;
  sector: string | null;
  price: number;
  percent: number;
  history30d: StockPricePoint[];
  ohlc30d: OHLCPoint[];
}

export interface StockNewsItem {
  title: string;
  summary: string;
  source: string;
  publishedAt: string | null;
  url: string | null;
}

interface BackendMover {
  ticker?: string | null;
  symbol?: string | null;
  price?: number | null;
  daily_change?: number | null;
  change_percent?: number | null;
  percent?: number | null;
}

interface BackendStockSearchResult {
  symbol: string;
  name: string | null;
  exchange: string | null;
  sector: string | null;
  price?: number | null;
  daily_change?: number | null;
}

interface BackendStockPrice {
  date: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
}

interface BackendStockPriceResponse {
  data?: {
    prices?: BackendStockPrice[];
  };
}

interface BackendNewsArticle {
  title: string;
  summary: string | null;
  url: string | null;
  source: string | null;
  published_at: string | null;
}

interface BackendTopMovers {
  gainers?: BackendMover[];
  losers?: BackendMover[];
}

interface BackendSectorMarketCap {
  industry: string;
  weight: number;
}

interface BackendSectorDetail {
  industry: string;
  avg_growth_1w: number | null;
}

const stockSearchMockData: StockSearchResult[] = [
  { symbol: "FPT", name: "FPT Corporation", exchange: "HOSE", sector: "Công nghệ", price: 132500, percent: 2.71 },
  { symbol: "MWG", name: "CTCP Đầu tư Thế Giới Di Động", exchange: "HOSE", sector: "Bán lẻ", price: 56200, percent: 3.31 },
  { symbol: "VCB", name: "Ngân hàng TMCP Ngoại thương Việt Nam", exchange: "HOSE", sector: "Ngân hàng", price: 85400, percent: 1.43 },
  { symbol: "TCB", name: "Ngân hàng TMCP Kỹ thương Việt Nam", exchange: "HOSE", sector: "Ngân hàng", price: 35700, percent: 2 },
  { symbol: "HPG", name: "CTCP Tập đoàn Hòa Phát", exchange: "HOSE", sector: "Thép", price: 28350, percent: -2.91 },
  { symbol: "SSI", name: "CTCP Chứng khoán SSI", exchange: "HOSE", sector: "Tài chính", price: 32100, percent: -2.13 },
  { symbol: "VNM", name: "CTCP Sữa Việt Nam", exchange: "HOSE", sector: "Thực phẩm", price: 72600, percent: -0.55 },
  { symbol: "VRE", name: "CTCP Vincom Retail", exchange: "HOSE", sector: "Bất động sản", price: 28900, percent: -1.7 },
  { symbol: "ACB", name: "Ngân hàng TMCP Á Châu", exchange: "HOSE", sector: "Ngân hàng", price: 25100, percent: 0.95 },
  { symbol: "MBB", name: "Ngân hàng TMCP Quân đội", exchange: "HOSE", sector: "Ngân hàng", price: 26200, percent: 1.8 },
  { symbol: "VIC", name: "Tập đoàn Vingroup", exchange: "HOSE", sector: "Bất động sản", price: 42700, percent: 1.1 },
  { symbol: "BSR", name: "Lọc hóa dầu Bình Sơn", exchange: "UPCOM", sector: "Dầu khí", price: 21900, percent: 1.2 },
  { symbol: "PVS", name: "Dịch vụ kỹ thuật dầu khí", exchange: "HNX", sector: "Dầu khí", price: 34900, percent: 1.9 },
  { symbol: "DGC", name: "Hóa chất Đức Giang", exchange: "HOSE", sector: "Hóa chất", price: 112400, percent: 2.3 },
  { symbol: "VIX", name: "Chứng khoán VIX", exchange: "HOSE", sector: "Tài chính", price: 18100, percent: 4.6 },
];

const stockNewsMockData: Record<string, StockNewsItem[]> = {
  FPT: [
    {
      title: "FPT công bố doanh thu quý gần nhất tăng trưởng tích cực",
      summary: "Kết quả kinh doanh vượt kỳ vọng nhờ mảng công nghệ và chuyển đổi số.",
      source: "CafeF",
      publishedAt: "2026-04-05T08:00:00Z",
      url: "https://cafef.vn/",
    },
    {
      title: "Khối ngoại duy trì mua ròng cổ phiếu FPT",
      summary: "Thanh khoản tăng mạnh, dòng tiền tổ chức cải thiện trong nhiều phiên.",
      source: "VnEconomy",
      publishedAt: "2026-04-04T02:30:00Z",
      url: "https://vneconomy.vn/",
    },
  ],
};

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function formatDateParam(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function computeMockBasePrice(symbol: string): number {
  const fromMovers = [...marketMock.topGainers, ...marketMock.topLosers].find((stock) => stock.symbol === symbol);
  if (fromMovers) {
    return fromMovers.price > 1000 ? fromMovers.price / 1000 : fromMovers.price;
  }

  return 25;
}

function buildMockPriceHistory(symbol: string, days: number): StockPricePoint[] {
  const safeDays = Math.max(1, days);
  const seed = symbol.split("").reduce((acc, char) => acc + char.charCodeAt(0), 0);
  const basePrice = computeMockBasePrice(symbol);
  const drift = ((seed % 9) - 4) * 0.0008;
  const today = new Date();

  const data: StockPricePoint[] = [];
  for (let index = safeDays - 1; index >= 0; index -= 1) {
    const pointDate = new Date(today);
    pointDate.setDate(today.getDate() - index);

    const dayNo = safeDays - index;
    const seasonal = Math.sin((dayNo + seed) / 3) * 0.012;
    const trend = dayNo * drift;
    const close = Math.max(1000, Math.round(basePrice * (1 + seasonal + trend)));

    data.push({
      date: formatDateParam(pointDate),
      close,
    });
  }

  return data;
}

function buildMockOHLCHistory(symbol: string, days: number): OHLCPoint[] {
  const seed = symbol.split("").reduce((acc, char) => acc + char.charCodeAt(0), 0);

  return buildMockPriceHistory(symbol, days).map((point, index) => {
    const open = Math.max(1000, Math.round(point.close * 0.99));
    const high = Math.max(open, Math.round(point.close * 1.01));
    const low = Math.max(1000, Math.round(point.close * 0.98));
    const volume = 900000 + ((seed + index) % 30) * 25000;

    return {
      date: point.date,
      open,
      high,
      low,
      close: point.close,
      volume,
    };
  });
}

function mapNewsArticle(article: BackendNewsArticle): StockNewsItem {
  return {
    title: article.title,
    summary: article.summary ?? "",
    source: article.source ?? "Không rõ",
    publishedAt: article.published_at,
    url: article.url,
  };
}

function toStockBasic(stock: BackendMover): StockBasic | null {
  const symbol = (stock.ticker ?? stock.symbol ?? "").trim().toUpperCase();
  const rawPrice = stock.price;
  const rawPercent = stock.daily_change ?? stock.change_percent ?? stock.percent;

  if (!symbol || typeof rawPrice !== "number" || !Number.isFinite(rawPrice)) {
    return null;
  }

  const percent = typeof rawPercent === "number" && Number.isFinite(rawPercent) ? rawPercent : 0;

  return {
    symbol,
    price: rawPrice,
    percent,
  };
}

function mapMovers(stocks: BackendMover[] | undefined): StockBasic[] {
  if (!Array.isArray(stocks) || stocks.length === 0) {
    return [];
  }

  return stocks
    .map(toStockBasic)
    .filter((stock): stock is StockBasic => stock !== null);
}

function ensureMoversData(topGainers: StockBasic[], topLosers: StockBasic[]): { topGainers: StockBasic[]; topLosers: StockBasic[] } {
  const mergedBySymbol = new Map<string, StockBasic>();

  for (const stock of [...topGainers, ...topLosers]) {
    if (!mergedBySymbol.has(stock.symbol)) {
      mergedBySymbol.set(stock.symbol, stock);
    }
  }

  const merged = [...mergedBySymbol.values()];
  const fallbackSize = Math.max(topGainers.length, topLosers.length, 10);
  const fallbackGainers = [...merged].sort((a, b) => b.percent - a.percent).slice(0, fallbackSize);
  const fallbackLosers = [...merged].sort((a, b) => a.percent - b.percent).slice(0, fallbackSize);

  const resolvedGainers = topGainers.length > 0 ? topGainers : fallbackGainers;
  const resolvedLosers = topLosers.length > 0 ? topLosers : fallbackLosers;

  if (resolvedGainers.length > 0 || resolvedLosers.length > 0) {
    return {
      topGainers: resolvedGainers,
      topLosers: resolvedLosers,
    };
  }

  return {
    topGainers: marketMock.topGainers,
    topLosers: marketMock.topLosers,
  };
}

async function getLiveMarketData(): Promise<MarketData> {
  const [movers, marketCap, sectorDetail] = await Promise.all([
    apiGet<BackendTopMovers>("/api/v1/market/top-movers?top_n=10"),
    apiGet<BackendSectorMarketCap[]>("/api/v1/market/sectors/market-cap?top_n=10"),
    apiGet<BackendSectorDetail[]>("/api/v1/market/sectors/detail"),
  ]);

  const changeMap = new Map(sectorDetail.map((sector) => [sector.industry, sector.avg_growth_1w ?? 0]));

  const sectors: Sector[] = marketCap.map((sector) => ({
    name: sector.industry,
    size: Math.round(sector.weight * 100),
    change: changeMap.get(sector.industry) ?? 0,
  }));

  const mappedGainers = mapMovers(movers.gainers);
  const mappedLosers = mapMovers(movers.losers);
  const { topGainers, topLosers } = ensureMoversData(mappedGainers, mappedLosers);

  return {
    sectors,
    topGainers,
    topLosers,
    foreignFlow: marketMock.foreignFlow,
  };
}

export async function getMarketData(): Promise<MarketData> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return marketMock;
  }

  return getLiveMarketData();
}

export async function searchStocks(query: string, limit = 8): Promise<StockSearchResult[]> {
  const normalized = query.trim();
  if (!normalized) {
    return [];
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);

    const lowerQuery = normalized.toLowerCase();
    return stockSearchMockData
      .filter((stock) =>
        stock.symbol.toLowerCase().includes(lowerQuery)
        || (stock.name ?? "").toLowerCase().includes(lowerQuery)
      )
      .slice(0, limit);
  }

  const results = await apiGet<BackendStockSearchResult[]>(
    `/api/v1/market/search?query=${encodeURIComponent(normalized)}&limit=${limit}`,
  );

  return results.map((item) => ({
    symbol: item.symbol,
    name: item.name,
    exchange: item.exchange,
    sector: item.sector,
    price: typeof item.price === "number" ? item.price : null,
    percent: typeof item.daily_change === "number" ? item.daily_change : null,
  }));
}

export async function getStockPriceHistory(symbol: string, days = 30): Promise<StockPricePoint[]> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    return [];
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return buildMockPriceHistory(normalizedSymbol, days);
  }

  const safeDays = Math.max(1, days);
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - Math.max(safeDays * 2, 45));

  const response = await apiGet<BackendStockPriceResponse>(
    `/api/v1/market/stock/${encodeURIComponent(normalizedSymbol)}/price?start_date=${formatDateParam(startDate)}&end_date=${formatDateParam(endDate)}`,
  );

  const prices = response.data?.prices ?? [];
  return prices
    .filter((point): point is BackendStockPrice & { close: number } => typeof point.close === "number")
    .map((point) => ({
      date: point.date,
      close: point.close,
    }))
    .sort((a, b) => a.date.localeCompare(b.date))
    .slice(-safeDays);
}

export async function getStockOHLC(symbol: string, days = 30): Promise<OHLCPoint[]> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    return [];
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return buildMockOHLCHistory(normalizedSymbol, days);
  }

  const safeDays = Math.max(1, days);
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - Math.max(safeDays * 2, 45));

  const response = await apiGet<BackendStockPriceResponse>(
    `/api/v1/market/stock/${encodeURIComponent(normalizedSymbol)}/price?start_date=${formatDateParam(startDate)}&end_date=${formatDateParam(endDate)}`,
  );

  const prices = response.data?.prices ?? [];

  return prices
    .filter(
      (point): point is BackendStockPrice & { open: number; high: number; low: number; close: number } => (
        typeof point.open === "number"
        && typeof point.high === "number"
        && typeof point.low === "number"
        && typeof point.close === "number"
      ),
    )
    .map((point) => ({
      date: point.date,
      open: point.open,
      high: point.high,
      low: point.low,
      close: point.close,
      volume: typeof point.volume === "number" ? point.volume : 0,
    }))
    .sort((a, b) => a.date.localeCompare(b.date))
    .slice(-safeDays);
}

export async function getStockDetail(symbol: string): Promise<StockDetailData> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    throw new Error("Missing stock symbol");
  }

  const [searchResults, history30d, ohlc30d] = await Promise.all([
    searchStocks(normalizedSymbol, 8),
    getStockPriceHistory(normalizedSymbol, 30),
    getStockOHLC(normalizedSymbol, 30),
  ]);

  const matched =
    searchResults.find((stock) => stock.symbol.toUpperCase() === normalizedSymbol)
    ?? searchResults[0]
    ?? {
      symbol: normalizedSymbol,
      name: null,
      exchange: null,
      sector: null,
      price: null,
      percent: null,
    };

  const latestClose = history30d[history30d.length - 1]?.close ?? 0;
  const previousClose = history30d[history30d.length - 2]?.close ?? latestClose;
  const percent = previousClose > 0 ? ((latestClose - previousClose) / previousClose) * 100 : 0;

  return {
    symbol: matched.symbol,
    name: matched.name,
    exchange: matched.exchange,
    sector: matched.sector,
    price: latestClose,
    percent,
    history30d,
    ohlc30d,
  };
}

export async function getStockNews(symbol: string, limit = 5): Promise<StockNewsItem[]> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    return [];
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return (stockNewsMockData[normalizedSymbol] ?? []).slice(0, limit);
  }

  const news = await apiGet<BackendNewsArticle[]>(
    `/api/v1/market/news/${encodeURIComponent(normalizedSymbol)}?limit=${limit}`,
  );
  return news.map(mapNewsArticle);
}
