import type { StockSearchResult } from "@/repositories/marketRepository";

const DEFAULT_HISTORY_KEY = "portfolio:stock-search-history";
const DEFAULT_HISTORY_LIMIT = 20;

export interface StockSearchHistoryItem extends StockSearchResult {
  searchedAt: number;
}

function canUseStorage(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

function normalizeItem(item: unknown): StockSearchHistoryItem | null {
  if (!item || typeof item !== "object") {
    return null;
  }

  const parsed = item as Partial<StockSearchHistoryItem>;
  const symbol = typeof parsed.symbol === "string" ? parsed.symbol.trim().toUpperCase() : "";
  if (!symbol) {
    return null;
  }

  return {
    symbol,
    name: typeof parsed.name === "string" ? parsed.name : null,
    exchange: typeof parsed.exchange === "string" ? parsed.exchange : null,
    sector: typeof parsed.sector === "string" ? parsed.sector : null,
    price: typeof parsed.price === "number" ? parsed.price : null,
    percent: typeof parsed.percent === "number" ? parsed.percent : null,
    searchedAt: typeof parsed.searchedAt === "number" ? parsed.searchedAt : Date.now(),
  };
}

function readRawHistory(storageKey: string): StockSearchHistoryItem[] {
  if (!canUseStorage()) {
    return [];
  }

  const raw = window.localStorage.getItem(storageKey);
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .map((item) => normalizeItem(item))
      .filter((item): item is StockSearchHistoryItem => Boolean(item));
  } catch {
    return [];
  }
}

function writeRawHistory(storageKey: string, history: StockSearchHistoryItem[]): void {
  if (!canUseStorage()) {
    return;
  }

  window.localStorage.setItem(storageKey, JSON.stringify(history));
}

export function getStockSearchHistory(
  storageKey = DEFAULT_HISTORY_KEY,
  limit = DEFAULT_HISTORY_LIMIT,
): StockSearchHistoryItem[] {
  return readRawHistory(storageKey)
    .sort((left, right) => right.searchedAt - left.searchedAt)
    .slice(0, Math.max(1, limit));
}

export function saveStockSearchHistory(
  stock: StockSearchResult,
  storageKey = DEFAULT_HISTORY_KEY,
  limit = DEFAULT_HISTORY_LIMIT,
): StockSearchHistoryItem[] {
  const normalizedSymbol = stock.symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    return getStockSearchHistory(storageKey, limit);
  }

  const nextItem: StockSearchHistoryItem = {
    ...stock,
    symbol: normalizedSymbol,
    searchedAt: Date.now(),
  };

  const current = readRawHistory(storageKey).filter((item) => item.symbol !== normalizedSymbol);
  const next = [nextItem, ...current].slice(0, Math.max(1, limit));
  writeRawHistory(storageKey, next);
  return next;
}

export function removeStockSearchHistory(
  symbol: string,
  storageKey = DEFAULT_HISTORY_KEY,
  limit = DEFAULT_HISTORY_LIMIT,
): StockSearchHistoryItem[] {
  const normalizedSymbol = symbol.trim().toUpperCase();
  const next = readRawHistory(storageKey)
    .filter((item) => item.symbol !== normalizedSymbol)
    .slice(0, Math.max(1, limit));

  writeRawHistory(storageKey, next);
  return next;
}

export function clearStockSearchHistory(storageKey = DEFAULT_HISTORY_KEY): void {
  if (!canUseStorage()) {
    return;
  }

  window.localStorage.removeItem(storageKey);
}
