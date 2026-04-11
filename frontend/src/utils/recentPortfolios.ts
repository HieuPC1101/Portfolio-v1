const RECENT_PORTFOLIO_STORAGE_KEY = "portfolio:recent-ids";
const MAX_RECENT_PORTFOLIOS = 5;

function normalizeRecentPortfolioIds(rawValue: unknown): string[] {
  if (!Array.isArray(rawValue)) {
    return [];
  }

  const normalized = rawValue
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter(Boolean);

  return Array.from(new Set(normalized));
}

export function readRecentPortfolioIds(): string[] {
  if (typeof window === "undefined") {
    return [];
  }

  const raw = localStorage.getItem(RECENT_PORTFOLIO_STORAGE_KEY);
  if (!raw) {
    return [];
  }

  try {
    return normalizeRecentPortfolioIds(JSON.parse(raw));
  } catch {
    return [];
  }
}

export function saveRecentPortfolioIds(ids: string[]): void {
  if (typeof window === "undefined") {
    return;
  }

  const normalized = normalizeRecentPortfolioIds(ids).slice(0, MAX_RECENT_PORTFOLIOS);
  localStorage.setItem(RECENT_PORTFOLIO_STORAGE_KEY, JSON.stringify(normalized));
}

export function pushRecentPortfolioId(id: string): string[] {
  const normalizedId = id.trim();
  if (!normalizedId) {
    return readRecentPortfolioIds();
  }

  const current = readRecentPortfolioIds();
  if (current[0] === normalizedId) {
    return current;
  }

  const next = [normalizedId, ...current.filter((item) => item !== normalizedId)].slice(0, MAX_RECENT_PORTFOLIOS);
  saveRecentPortfolioIds(next);
  return next;
}
