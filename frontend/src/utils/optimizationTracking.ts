const OPTIMIZATION_TRACK_STORAGE_KEY = "optimize:track-history";
const MAX_TRACK_ENTRIES = 20;

export interface OptimizationTrackEntry {
  portfolioId: string;
  portfolioName: string;
  algorithmId: string;
  algorithmName: string;
  expectedReturn: number;
  volatility: number;
  sharpeRatio: number;
  topWeightsSummary: string;
  createdAt: string;
}

function normalizeEntry(value: unknown): OptimizationTrackEntry | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const candidate = value as Partial<OptimizationTrackEntry>;
  if (
    typeof candidate.portfolioId !== "string"
    || typeof candidate.portfolioName !== "string"
    || typeof candidate.algorithmId !== "string"
    || typeof candidate.algorithmName !== "string"
    || typeof candidate.expectedReturn !== "number"
    || typeof candidate.volatility !== "number"
    || typeof candidate.sharpeRatio !== "number"
    || typeof candidate.topWeightsSummary !== "string"
    || typeof candidate.createdAt !== "string"
  ) {
    return null;
  }

  return {
    portfolioId: candidate.portfolioId,
    portfolioName: candidate.portfolioName,
    algorithmId: candidate.algorithmId,
    algorithmName: candidate.algorithmName,
    expectedReturn: candidate.expectedReturn,
    volatility: candidate.volatility,
    sharpeRatio: candidate.sharpeRatio,
    topWeightsSummary: candidate.topWeightsSummary,
    createdAt: candidate.createdAt,
  };
}

function sanitizeEntries(rawValue: unknown): OptimizationTrackEntry[] {
  if (!Array.isArray(rawValue)) {
    return [];
  }

  return rawValue
    .map(normalizeEntry)
    .filter((item): item is OptimizationTrackEntry => item !== null)
    .slice(0, MAX_TRACK_ENTRIES);
}

export function readOptimizationTracks(): OptimizationTrackEntry[] {
  if (typeof window === "undefined") {
    return [];
  }

  const raw = localStorage.getItem(OPTIMIZATION_TRACK_STORAGE_KEY);
  if (!raw) {
    return [];
  }

  try {
    return sanitizeEntries(JSON.parse(raw));
  } catch {
    return [];
  }
}

export function saveOptimizationTracks(entries: OptimizationTrackEntry[]): void {
  if (typeof window === "undefined") {
    return;
  }

  localStorage.setItem(
    OPTIMIZATION_TRACK_STORAGE_KEY,
    JSON.stringify(sanitizeEntries(entries)),
  );
}

export function pushOptimizationTrack(entry: OptimizationTrackEntry): OptimizationTrackEntry[] {
  const current = readOptimizationTracks();
  const deduped = current.filter((item) => item.createdAt !== entry.createdAt);
  const next = [entry, ...deduped].slice(0, MAX_TRACK_ENTRIES);
  saveOptimizationTracks(next);
  return next;
}

export function clearOptimizationTracks(): void {
  if (typeof window === "undefined") {
    return;
  }

  localStorage.removeItem(OPTIMIZATION_TRACK_STORAGE_KEY);
}
