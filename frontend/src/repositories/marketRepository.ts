import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiGet } from "@/lib/api";
import { decodeHtmlEntities } from "@/lib/format";
import { marketMock } from "@/mocks/market.mock";
import type { MarketData, Sector, StockBasic } from "@/types/market";
import type {
  FinancialPeriod,
  StockFinancialPeriod,
  StockFinancialsData,
  StockOverview,
  StockRatios,
} from "@/types/stockDetail";

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

interface BackendStockOverviewResponse {
  symbol: string;
  company_name?: string | null;
  exchange?: string | null;
  sector?: string | null;
  industry?: string | null;
  market_cap?: number | null;
  shares_outstanding?: number | null;
  free_float?: number | null;
  listing_date?: string | null;
  headquarters?: string | null;
  employee_count?: number | null;
  business_summary?: string | null;
  latest_highlights?: string[] | null;
  as_of_date?: string | null;
  source?: string | null;
}

interface BackendStockFinancialPeriod {
  period_label: string;
  year?: number | null;
  quarter?: number | null;
  revenue?: number | null;
  gross_profit?: number | null;
  operating_profit?: number | null;
  net_income?: number | null;
  total_assets?: number | null;
  total_liabilities?: number | null;
  equity?: number | null;
  total_debt?: number | null;
  cash_and_cash_equivalents?: number | null;
  operating_cash_flow?: number | null;
  investing_cash_flow?: number | null;
  financing_cash_flow?: number | null;
  free_cash_flow?: number | null;
}

interface BackendStockFinancialsResponse {
  symbol: string;
  period: string;
  items?: BackendStockFinancialPeriod[];
  as_of_date?: string | null;
  source?: string | null;
}

interface BackendStockRatiosResponse {
  symbol: string;
  pe?: number | null;
  pb?: number | null;
  ev_ebitda?: number | null;
  gross_margin?: number | null;
  net_margin?: number | null;
  roe?: number | null;
  roa?: number | null;
  debt_to_equity?: number | null;
  as_of_date?: string | null;
  reporting_period?: string | null;
  quality_flags?: string[] | null;
  source?: string | null;
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

const stockOverviewMockData: Record<string, StockOverview> = {
  FPT: {
    symbol: "FPT",
    companyName: "CTCP FPT",
    exchange: "HOSE",
    sector: "Công nghệ",
    industry: "Công nghệ thông tin",
    marketCap: 185_000_000_000_000,
    sharesOutstanding: 1_470_000_000,
    freeFloat: 0.72,
    listingDate: "2006-12-13",
    headquarters: "Hà Nội, Việt Nam",
    employeeCount: 50_000,
    businessSummary: "Doanh nghiệp công nghệ và viễn thông hàng đầu với trọng tâm chuyển đổi số và xuất khẩu phần mềm.",
    latestHighlights: [
      "Doanh thu mảng công nghệ tăng trưởng ổn định trong các quý gần đây.",
      "Biên lợi nhuận duy trì tích cực nhờ dịch vụ chuyển đổi số.",
      "Dòng tiền hoạt động dương và ổn định.",
    ],
    asOfDate: "2026-03-31",
    source: "mock",
  },
};

const stockRatiosMockData: Record<string, StockRatios> = {
  FPT: {
    symbol: "FPT",
    pe: 18.4,
    pb: 4.1,
    evEbitda: 11.7,
    grossMargin: 0.41,
    netMargin: 0.16,
    roe: 0.24,
    roa: 0.12,
    debtToEquity: 0.45,
    asOfDate: "2026-03-31",
    reportingPeriod: "Q4/2025",
    qualityFlags: [],
    source: "mock",
  },
};

const stockFinancialsMockData: Record<FinancialPeriod, Record<string, StockFinancialPeriod[]>> = {
  quarterly: {
    FPT: [
      {
        periodLabel: "Q4/2025",
        year: 2025,
        quarter: 4,
        revenue: 18_000_000_000_000,
        grossProfit: 7_300_000_000_000,
        operatingProfit: 3_900_000_000_000,
        netIncome: 2_900_000_000_000,
        totalAssets: 70_000_000_000_000,
        totalLiabilities: 34_000_000_000_000,
        equity: 36_000_000_000_000,
        totalDebt: 12_000_000_000_000,
        cashAndCashEquivalents: 8_500_000_000_000,
        operatingCashFlow: 3_100_000_000_000,
        investingCashFlow: -1_200_000_000_000,
        financingCashFlow: 500_000_000_000,
        freeCashFlow: 1_900_000_000_000,
      },
      {
        periodLabel: "Q3/2025",
        year: 2025,
        quarter: 3,
        revenue: 17_100_000_000_000,
        grossProfit: 6_900_000_000_000,
        operatingProfit: 3_600_000_000_000,
        netIncome: 2_700_000_000_000,
        totalAssets: 68_500_000_000_000,
        totalLiabilities: 33_500_000_000_000,
        equity: 35_000_000_000_000,
        totalDebt: 11_800_000_000_000,
        cashAndCashEquivalents: 8_200_000_000_000,
        operatingCashFlow: 2_800_000_000_000,
        investingCashFlow: -1_100_000_000_000,
        financingCashFlow: 450_000_000_000,
        freeCashFlow: 1_700_000_000_000,
      },
    ],
  },
  yearly: {
    FPT: [
      {
        periodLabel: "2025",
        year: 2025,
        quarter: null,
        revenue: 68_000_000_000_000,
        grossProfit: 27_800_000_000_000,
        operatingProfit: 15_100_000_000_000,
        netIncome: 11_600_000_000_000,
        totalAssets: 70_000_000_000_000,
        totalLiabilities: 34_000_000_000_000,
        equity: 36_000_000_000_000,
        totalDebt: 12_000_000_000_000,
        cashAndCashEquivalents: 8_500_000_000_000,
        operatingCashFlow: 12_300_000_000_000,
        investingCashFlow: -4_200_000_000_000,
        financingCashFlow: 1_800_000_000_000,
        freeCashFlow: 8_100_000_000_000,
      },
    ],
  },
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
    title: decodeHtmlEntities(article.title),
    summary: decodeHtmlEntities(article.summary ?? ""),
    source: decodeHtmlEntities(article.source ?? "Không rõ"),
    publishedAt: article.published_at,
    url: article.url?.trim() || null,
  };
}

function getDefaultStockOverview(symbol: string): StockOverview {
  return {
    symbol,
    companyName: null,
    exchange: null,
    sector: null,
    industry: null,
    marketCap: null,
    sharesOutstanding: null,
    freeFloat: null,
    listingDate: null,
    headquarters: null,
    employeeCount: null,
    businessSummary: null,
    latestHighlights: [],
    asOfDate: null,
    source: null,
  };
}

function getDefaultStockRatios(symbol: string): StockRatios {
  return {
    symbol,
    pe: null,
    pb: null,
    evEbitda: null,
    grossMargin: null,
    netMargin: null,
    roe: null,
    roa: null,
    debtToEquity: null,
    asOfDate: null,
    reportingPeriod: null,
    qualityFlags: [],
    source: null,
  };
}

function mapStockOverview(response: BackendStockOverviewResponse): StockOverview {
  return {
    symbol: response.symbol,
    companyName: response.company_name ?? null,
    exchange: response.exchange ?? null,
    sector: response.sector ?? null,
    industry: response.industry ?? null,
    marketCap: typeof response.market_cap === "number" ? response.market_cap : null,
    sharesOutstanding: typeof response.shares_outstanding === "number" ? response.shares_outstanding : null,
    freeFloat: typeof response.free_float === "number" ? response.free_float : null,
    listingDate: response.listing_date ?? null,
    headquarters: response.headquarters ?? null,
    employeeCount: typeof response.employee_count === "number" ? response.employee_count : null,
    businessSummary: response.business_summary ?? null,
    latestHighlights: Array.isArray(response.latest_highlights) ? response.latest_highlights : [],
    asOfDate: response.as_of_date ?? null,
    source: response.source ?? null,
  };
}

function mapStockFinancialPeriod(item: BackendStockFinancialPeriod): StockFinancialPeriod {
  return {
    periodLabel: item.period_label,
    year: typeof item.year === "number" ? item.year : null,
    quarter: typeof item.quarter === "number" ? item.quarter : null,
    revenue: typeof item.revenue === "number" ? item.revenue : null,
    grossProfit: typeof item.gross_profit === "number" ? item.gross_profit : null,
    operatingProfit: typeof item.operating_profit === "number" ? item.operating_profit : null,
    netIncome: typeof item.net_income === "number" ? item.net_income : null,
    totalAssets: typeof item.total_assets === "number" ? item.total_assets : null,
    totalLiabilities: typeof item.total_liabilities === "number" ? item.total_liabilities : null,
    equity: typeof item.equity === "number" ? item.equity : null,
    totalDebt: typeof item.total_debt === "number" ? item.total_debt : null,
    cashAndCashEquivalents: typeof item.cash_and_cash_equivalents === "number" ? item.cash_and_cash_equivalents : null,
    operatingCashFlow: typeof item.operating_cash_flow === "number" ? item.operating_cash_flow : null,
    investingCashFlow: typeof item.investing_cash_flow === "number" ? item.investing_cash_flow : null,
    financingCashFlow: typeof item.financing_cash_flow === "number" ? item.financing_cash_flow : null,
    freeCashFlow: typeof item.free_cash_flow === "number" ? item.free_cash_flow : null,
  };
}

function mapStockFinancials(response: BackendStockFinancialsResponse): StockFinancialsData {
  const period: FinancialPeriod = response.period.toLowerCase().includes("year") ? "yearly" : "quarterly";
  return {
    symbol: response.symbol,
    period,
    items: (response.items ?? []).map(mapStockFinancialPeriod),
    asOfDate: response.as_of_date ?? null,
    source: response.source ?? null,
  };
}

function mapStockRatios(response: BackendStockRatiosResponse): StockRatios {
  return {
    symbol: response.symbol,
    pe: typeof response.pe === "number" ? response.pe : null,
    pb: typeof response.pb === "number" ? response.pb : null,
    evEbitda: typeof response.ev_ebitda === "number" ? response.ev_ebitda : null,
    grossMargin: typeof response.gross_margin === "number" ? response.gross_margin : null,
    netMargin: typeof response.net_margin === "number" ? response.net_margin : null,
    roe: typeof response.roe === "number" ? response.roe : null,
    roa: typeof response.roa === "number" ? response.roa : null,
    debtToEquity: typeof response.debt_to_equity === "number" ? response.debt_to_equity : null,
    asOfDate: response.as_of_date ?? null,
    reportingPeriod: response.reporting_period ?? null,
    qualityFlags: Array.isArray(response.quality_flags) ? response.quality_flags : [],
    source: response.source ?? null,
  };
}

export function getFinancialPeriodLabel(item: Pick<StockFinancialPeriod, "periodLabel" | "year" | "quarter">): string {
  const fromField = (item.periodLabel ?? "").trim();
  if (fromField) {
    return fromField;
  }

  if (typeof item.year === "number" && typeof item.quarter === "number") {
    return `Q${item.quarter}/${item.year}`;
  }

  if (typeof item.year === "number") {
    return `${item.year}`;
  }

  return "--";
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

export async function getStockOverview(symbol: string): Promise<StockOverview> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    throw new Error("Missing stock symbol");
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return stockOverviewMockData[normalizedSymbol] ?? getDefaultStockOverview(normalizedSymbol);
  }

  const overview = await apiGet<BackendStockOverviewResponse>(
    `/api/v1/market/stock/${encodeURIComponent(normalizedSymbol)}/overview`,
  );
  return mapStockOverview(overview);
}

export async function getStockFinancials(
  symbol: string,
  period: FinancialPeriod = "quarterly",
  limit = 8,
): Promise<StockFinancialsData> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    throw new Error("Missing stock symbol");
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return {
      symbol: normalizedSymbol,
      period,
      items: (stockFinancialsMockData[period][normalizedSymbol] ?? []).slice(0, limit),
      asOfDate: "2026-03-31",
      source: "mock",
    };
  }

  const financials = await apiGet<BackendStockFinancialsResponse>(
    `/api/v1/market/stock/${encodeURIComponent(normalizedSymbol)}/financials?period=${period}&limit=${Math.max(1, limit)}`,
  );
  return mapStockFinancials(financials);
}

export async function getStockRatios(symbol: string): Promise<StockRatios> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    throw new Error("Missing stock symbol");
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return stockRatiosMockData[normalizedSymbol] ?? getDefaultStockRatios(normalizedSymbol);
  }

  const ratios = await apiGet<BackendStockRatiosResponse>(
    `/api/v1/market/stock/${encodeURIComponent(normalizedSymbol)}/ratios`,
  );
  return mapStockRatios(ratios);
}
