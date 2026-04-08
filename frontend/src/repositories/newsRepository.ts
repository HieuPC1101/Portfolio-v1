import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiGet } from "@/lib/api";
import { formatRelativeTime } from "@/lib/format";
import { newsMock } from "@/mocks/news.mock";
import type { NewsFeedData, NewsItem } from "@/types/news";

interface BackendNewsArticle {
  title: string;
  summary: string | null;
  url: string | null;
  source: string | null;
  published_at: string | null;
  category: string | null;
}

function mapNewsArticle(article: BackendNewsArticle, index: number): NewsItem {
  return {
    id: String(index + 1),
    title: article.title,
    summary: article.summary ?? "",
    source: article.source ?? "Không rõ",
    time: article.published_at ? formatRelativeTime(new Date(article.published_at)) : "",
    category: article.category ?? "Thị trường",
    url: article.url ?? undefined,
  };
}

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function getNewsFeed(): Promise<NewsFeedData> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return newsMock;
  }

  const articles = await apiGet<BackendNewsArticle[]>("/api/v1/market/news?limit=20");
  return { items: articles.map(mapNewsArticle) };
}
