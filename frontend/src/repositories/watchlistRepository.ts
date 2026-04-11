import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiGet } from "@/lib/api";
import { apiAuthDelete, apiAuthGet, apiAuthPost } from "@/lib/apiAuth";
import { watchlistMock } from "@/mocks/watchlist.mock";
import type { WatchlistData, WatchlistItem, WatchlistMutationPayload } from "@/types/watchlist";

const DEFAULT_WATCHLIST_NAME = "Danh sach theo doi";

interface BackendWatchlistStock {
  id: number;
  symbol: string;
}

interface BackendWatchlist {
  id: number;
  name: string;
  stocks: BackendWatchlistStock[];
}

interface BackendMover {
  ticker: string;
  price: number;
  daily_change: number;
}

interface BackendTopMovers {
  gainers: BackendMover[];
  losers: BackendMover[];
}

interface BackendStockPrice {
  date: string;
  close: number | null;
}

interface BackendStockPriceResponse {
  data?: {
    prices?: BackendStockPrice[];
  };
}

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function normalizeSymbol(symbol: string): string {
  return symbol.trim().toUpperCase();
}

function formatDateParam(date: Date): string {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function buildPriceMap(movers: BackendTopMovers): Map<string, { price: number; dailyChange: number }> {
  const priceMap = new Map<string, { price: number; dailyChange: number }>();

  for (const stock of [...movers.gainers, ...movers.losers]) {
    const ticker = normalizeSymbol(stock.ticker);
    if (!priceMap.has(ticker)) {
      priceMap.set(ticker, { price: stock.price, dailyChange: stock.daily_change });
    }
  }

  return priceMap;
}

async function getFallbackPrice(symbol: string): Promise<{ price: number; dailyChange: number } | null> {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - 14);

  try {
    const response = await apiGet<BackendStockPriceResponse>(
      `/api/v1/market/stock/${encodeURIComponent(symbol)}/price?start_date=${formatDateParam(startDate)}&end_date=${formatDateParam(endDate)}`,
    );

    const prices = (response.data?.prices ?? [])
      .filter((item): item is BackendStockPrice & { close: number } => typeof item.close === "number")
      .sort((a, b) => a.date.localeCompare(b.date));

    if (prices.length === 0) {
      return null;
    }

    const latestClose = prices[prices.length - 1]?.close ?? 0;
    const previousClose = prices[prices.length - 2]?.close ?? latestClose;
    const dailyChange = previousClose > 0
      ? ((latestClose - previousClose) / previousClose) * 100
      : 0;

    return {
      price: latestClose,
      dailyChange,
    };
  } catch {
    return null;
  }
}

async function toWatchlistItems(watchlists: BackendWatchlist[], movers: BackendTopMovers): Promise<WatchlistItem[]> {
  const orderedSymbols: string[] = [];
  const seenSymbols = new Set<string>();

  for (const watchlist of watchlists) {
    for (const stock of watchlist.stocks) {
      const symbol = normalizeSymbol(stock.symbol);
      if (!seenSymbols.has(symbol)) {
        seenSymbols.add(symbol);
        orderedSymbols.push(symbol);
      }
    }
  }

  const priceMap = buildPriceMap(movers);

  const fallbackQuotes = await Promise.all(
    orderedSymbols.map(async (symbol) => {
      const fromMovers = priceMap.get(symbol);
      if (fromMovers) {
        return [symbol, fromMovers] as const;
      }

      const fromPriceHistory = await getFallbackPrice(symbol);
      return [symbol, fromPriceHistory ?? { price: 0, dailyChange: 0 }] as const;
    }),
  );

  const resolvedMap = new Map(fallbackQuotes);

  return orderedSymbols.map((symbol) => {
    const price = resolvedMap.get(symbol);

    return {
      symbol,
      name: symbol,
      price: price?.price ?? 0,
      change: price ? Number((price.price * price.dailyChange / 100).toFixed(0)) : 0,
      percent: price?.dailyChange ?? 0,
    };
  });
}

async function fetchWatchlistsAndMovers(): Promise<[BackendWatchlist[], BackendTopMovers]> {
  return Promise.all([
    apiAuthGet<BackendWatchlist[]>("/api/v1/portfolios/watchlists"),
    apiGet<BackendTopMovers>("/api/v1/market/top-movers?top_n=50"),
  ]);
}

let mockState: WatchlistData = clone(watchlistMock);

async function getMockWatchlist(): Promise<WatchlistData> {
  await delay(MOCK_API_DELAY_MS);
  return clone(mockState);
}

async function addMockWatchlistSymbol(payload: WatchlistMutationPayload): Promise<void> {
  await delay(MOCK_API_DELAY_MS);

  const symbol = normalizeSymbol(payload.symbol);
  if (!symbol) {
    throw new Error("Ma co phieu khong hop le");
  }

  if (mockState.items.some((item) => normalizeSymbol(item.symbol) === symbol)) {
    throw new Error(`Ma ${symbol} da co trong watchlist`);
  }

  const seedItem = watchlistMock.items.find((item) => normalizeSymbol(item.symbol) === symbol);
  const nextItem: WatchlistItem = seedItem
    ? { ...seedItem, symbol }
    : {
      symbol,
      name: symbol,
      price: 0,
      change: 0,
      percent: 0,
    };

  mockState = {
    items: [nextItem, ...mockState.items],
  };
}

async function removeMockWatchlistSymbol(payload: WatchlistMutationPayload): Promise<void> {
  await delay(MOCK_API_DELAY_MS);

  const symbol = normalizeSymbol(payload.symbol);
  if (!symbol) {
    throw new Error("Ma co phieu khong hop le");
  }

  if (!mockState.items.some((item) => normalizeSymbol(item.symbol) === symbol)) {
    throw new Error(`Khong tim thay ma ${symbol} trong watchlist`);
  }

  mockState = {
    items: mockState.items.filter((item) => normalizeSymbol(item.symbol) !== symbol),
  };
}

async function ensurePrimaryWatchlist(watchlists: BackendWatchlist[]): Promise<BackendWatchlist> {
  if (watchlists.length > 0) {
    return watchlists[0];
  }

  return apiAuthPost<BackendWatchlist>("/api/v1/portfolios/watchlists", {
    name: DEFAULT_WATCHLIST_NAME,
  });
}

async function addLiveWatchlistSymbol(payload: WatchlistMutationPayload): Promise<void> {
  const symbol = normalizeSymbol(payload.symbol);
  if (!symbol) {
    throw new Error("Ma co phieu khong hop le");
  }

  const watchlists = await apiAuthGet<BackendWatchlist[]>("/api/v1/portfolios/watchlists");
  if (watchlists.some((watchlist) => watchlist.stocks.some((stock) => normalizeSymbol(stock.symbol) === symbol))) {
    throw new Error(`Ma ${symbol} da co trong watchlist`);
  }

  const target = await ensurePrimaryWatchlist(watchlists);
  await apiAuthPost<BackendWatchlistStock>(`/api/v1/portfolios/watchlists/${target.id}/stocks`, {
    symbol,
  });
}

async function removeLiveWatchlistSymbol(payload: WatchlistMutationPayload): Promise<void> {
  const symbol = normalizeSymbol(payload.symbol);
  if (!symbol) {
    throw new Error("Ma co phieu khong hop le");
  }

  const watchlists = await apiAuthGet<BackendWatchlist[]>("/api/v1/portfolios/watchlists");
  const targets = watchlists.filter(
    (watchlist) => watchlist.stocks.some((stock) => normalizeSymbol(stock.symbol) === symbol),
  );

  if (targets.length === 0) {
    throw new Error(`Khong tim thay ma ${symbol} trong watchlist`);
  }

  await Promise.all(
    targets.map((watchlist) =>
      apiAuthDelete(`/api/v1/portfolios/watchlists/${watchlist.id}/stocks/${encodeURIComponent(symbol)}`),
    ),
  );
}

async function getLiveWatchlist(): Promise<WatchlistData> {
  const [watchlists, movers] = await fetchWatchlistsAndMovers();
  return { items: await toWatchlistItems(watchlists, movers) };
}

export async function getWatchlist(): Promise<WatchlistData> {
  if (ENABLE_MOCK_API) {
    return getMockWatchlist();
  }

  return getLiveWatchlist();
}

export async function addWatchlistSymbol(payload: WatchlistMutationPayload): Promise<void> {
  if (ENABLE_MOCK_API) {
    await addMockWatchlistSymbol(payload);
    return;
  }

  await addLiveWatchlistSymbol(payload);
}

export async function removeWatchlistSymbol(payload: WatchlistMutationPayload): Promise<void> {
  if (ENABLE_MOCK_API) {
    await removeMockWatchlistSymbol(payload);
    return;
  }

  await removeLiveWatchlistSymbol(payload);
}
