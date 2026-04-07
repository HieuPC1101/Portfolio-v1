import { isMockMode, MOCK_DELAY_MS } from "@/config/env";
import { apiGet } from "@/lib/api";
import { apiAuthGet } from "@/lib/apiAuth";
import { watchlistMock } from "@/mocks/watchlist.mock";
import type { WatchlistData, WatchlistItem } from "@/types/watchlist";

interface BackendWatchlist {
  id: number;
  name: string;
  stocks: Array<{ id: number; symbol: string }>;
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

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function getLiveWatchlist(): Promise<WatchlistData> {
  const [watchlists, movers] = await Promise.all([
    apiAuthGet<BackendWatchlist[]>("/api/v1/portfolios/watchlists"),
    apiGet<BackendTopMovers>("/api/v1/market/top-movers?top_n=50"),
  ]);

  const symbols = watchlists.flatMap((watchlist) => watchlist.stocks.map((stock) => stock.symbol));
  const uniqueSymbols = new Set(symbols);

  const priceMap = new Map<string, { price: number; dailyChange: number }>();
  for (const stock of [...movers.gainers, ...movers.losers]) {
    if (!priceMap.has(stock.ticker)) {
      priceMap.set(stock.ticker, { price: stock.price, dailyChange: stock.daily_change });
    }
  }

  const items: WatchlistItem[] = [...uniqueSymbols].map((symbol) => {
    const price = priceMap.get(symbol);

    return {
      symbol,
      name: symbol,
      price: price?.price ?? 0,
      change: price ? parseFloat((price.price * price.dailyChange / 100).toFixed(0)) : 0,
      percent: price?.dailyChange ?? 0,
    };
  });

  return { items };
}

export async function getWatchlist(): Promise<WatchlistData> {
  if (isMockMode) {
    await delay(MOCK_DELAY_MS);
    return watchlistMock;
  }

  return getLiveWatchlist();
}
