export interface NewsItem {
  id: string;
  title: string;
  summary: string;
  source: string;
  time: string;
  category: string;
  url?: string;
}

export interface NewsFeedData {
  items: NewsItem[];
}
