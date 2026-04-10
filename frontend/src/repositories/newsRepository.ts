import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiGet } from "@/lib/api";
import { decodeHtmlEntities, formatRelativeTime, parseBackendDate } from "@/lib/format";
import { newsMock } from "@/mocks/news.mock";
import type { NewsFeedData, NewsItem } from "@/types/news";

interface BackendNewsArticle {
  title: string;
  summary: string | null;
  url: string | null;
  source: string | null;
  published_at: string | null;
  category: string | null;
  symbols?: string[] | null;
}

interface GetNewsFeedOptions {
  limit?: number;
  refresh?: boolean;
}

const SYMBOL_PATTERN = /\b[A-Z]{3,5}\b/g;
const SINGLE_SYMBOL_PATTERN = /^[A-Z]{3,5}$/;
const SYMBOL_STOPWORDS = new Set(["CTCP", "VN", "USD", "VND", "HOSE", "HNX", "UPCOM", "ETF"]);

function extractSymbolsFromText(text: string): string[] {
  const matches = text.match(SYMBOL_PATTERN) ?? [];
  const symbols = matches
    .map((symbol) => symbol.trim().toUpperCase())
    .filter((symbol) => !SYMBOL_STOPWORDS.has(symbol));

  return Array.from(new Set(symbols)).slice(0, 8);
}

function normalizeSymbols(article: BackendNewsArticle): string[] {
  const fromApi = Array.isArray(article.symbols)
    ? article.symbols
        .map((symbol) => String(symbol).trim().toUpperCase())
        .filter((symbol) => SINGLE_SYMBOL_PATTERN.test(symbol) && !SYMBOL_STOPWORDS.has(symbol))
    : [];

  if (fromApi.length > 0) {
    return Array.from(new Set(fromApi)).slice(0, 8);
  }

  const plainText = `${decodeHtmlEntities(article.title)} ${decodeHtmlEntities(article.summary ?? "")}`;
  return extractSymbolsFromText(plainText);
}

function mapNewsArticle(article: BackendNewsArticle, index: number): NewsItem {
  const publishedAt = parseBackendDate(article.published_at);
  const title = decodeHtmlEntities(article.title);
  const symbols = normalizeSymbols(article);

  return {
    id: article.url?.trim() || `${title}-${article.published_at ?? index}`,
    title,
    summary: decodeHtmlEntities(article.summary ?? ""),
    source: decodeHtmlEntities(article.source ?? "Không rõ"),
    time: publishedAt ? formatRelativeTime(publishedAt) : "Không rõ thời gian",
    publishedAt: article.published_at,
    symbols,
    category: decodeHtmlEntities(article.category ?? "Thị trường"),
    url: article.url?.trim() || undefined,
  };
}

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function getNewsFeed(options: GetNewsFeedOptions = {}): Promise<NewsFeedData> {
  const limit = options.limit ?? 20;
  const refresh = options.refresh ?? false;

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return newsMock;
  }

  const refreshQuery = refresh ? "&refresh=true" : "";
  const articles = await apiGet<BackendNewsArticle[]>(`/api/v1/market/news?limit=${limit}${refreshQuery}`);
  return { items: articles.map(mapNewsArticle) };
}
