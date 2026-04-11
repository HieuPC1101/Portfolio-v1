import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiGet } from "@/lib/api";
import { apiAuthGet, apiAuthPost } from "@/lib/apiAuth";
import { optimizeMock } from "@/mocks/optimize.mock";
import type {
  CalculateOptimizationPayload,
  OptimizeAlgorithm,
  OptimizeMetricSummary,
  OptimizePageData,
  OptimizeResultData,
  OptimizeStockSearchItem,
  OptimizeWeight,
} from "@/types/optimize";

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

interface BackendOptimizationModel {
  id: string;
  name: string;
  description?: string | null;
}

interface BackendOptimizationModelsResponse {
  models?: BackendOptimizationModel[];
}

interface LocalizedAlgorithmLabel {
  name: string;
  desc: string;
}

interface BackendStockSearchResult {
  symbol: string;
  name: string | null;
  exchange: string | null;
  sector: string | null;
}

interface BackendOptimizationRunResponse {
  model_name: string;
  input_symbols: string[];
  total_investment: number;
  expected_return: number | string | null;
  risk_volatility: number | string | null;
  expected_volatility?: number | string | null;
  expectedReturn?: number | string | null;
  riskVolatility?: number | string | null;
  volatility?: number | string | null;
  sharpe_ratio: number | string | null;
  sharpeRatio?: number | string | null;
  weights: Record<string, number>;
  shares: Record<string, number>;
  extra_data?: Record<string, unknown> | null;
}

interface BackendStockPrice {
  date: string;
  close?: number | null;
}

interface BackendStockPriceResponse {
  data?: {
    prices?: BackendStockPrice[];
  } | null;
}

interface BackendIndexHistoryPoint {
  time?: string | null;
  close?: number | null;
}

interface DatedClosePoint {
  date: string;
  close: number;
}

const BACKTEST_BENCHMARK_SYMBOL = "VNINDEX";
const DEFAULT_BACKTEST_DAYS = 120;

const stockSearchMockData: OptimizeStockSearchItem[] = [
  { symbol: "VCB", name: "Ngân hàng TMCP Ngoại thương Việt Nam", exchange: "HOSE" },
  { symbol: "FPT", name: "FPT Corporation", exchange: "HOSE" },
  { symbol: "VNM", name: "CTCP Sữa Việt Nam", exchange: "HOSE" },
  { symbol: "HPG", name: "CTCP Tập đoàn Hòa Phát", exchange: "HOSE" },
  { symbol: "MWG", name: "CTCP Đầu tư Thế Giới Di Động", exchange: "HOSE" },
  { symbol: "MBB", name: "Ngân hàng TMCP Quân đội", exchange: "HOSE" },
  { symbol: "SSI", name: "CTCP Chứng khoán SSI", exchange: "HOSE" },
];

const localizedAlgorithmLabels: Record<string, LocalizedAlgorithmLabel> = {
  markowitz: {
    name: "Markowitz (Trung bình - Phương sai)",
    desc: "Cân bằng giữa lợi nhuận kỳ vọng và mức rủi ro",
  },
  max_sharpe: {
    name: "Tối đa Sharpe Ratio",
    desc: "Tối ưu lợi nhuận điều chỉnh theo rủi ro",
  },
  min_volatility: {
    name: "Tối thiểu biến động",
    desc: "Giảm độ biến động danh mục theo hướng thận trọng",
  },
  min_vol: {
    name: "Tối thiểu biến động",
    desc: "Giảm độ biến động danh mục theo hướng thận trọng",
  },
  hrp: {
    name: "Phân bổ rủi ro phân cấp (HRP)",
    desc: "Đa dạng hóa theo cấu trúc phân cụm rủi ro",
  },
  min_cvar: {
    name: "Tối thiểu CVaR",
    desc: "Giảm tổn thất kỳ vọng trong các kịch bản xấu",
  },
  min_cdar: {
    name: "Tối thiểu CDaR",
    desc: "Giảm drawdown kỳ vọng trong các giai đoạn bất lợi",
  },
};

function normalizeAlgorithmId(value: string): string {
  return value.trim().toLowerCase().replace(/-/g, "_");
}

function localizeAlgorithmModel(model: BackendOptimizationModel): OptimizeAlgorithm {
  const normalizedId = normalizeAlgorithmId(model.id);
  const localized = localizedAlgorithmLabels[normalizedId];

  if (localized) {
    return {
      id: model.id,
      name: localized.name,
      desc: localized.desc,
    };
  }

  return {
    id: model.id,
    name: model.name,
    desc: model.description ?? "",
  };
}

function toNumber(value: number | string | null | undefined): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return undefined;
}

function toPercentValue(value: number | string | null | undefined): number {
  const numericValue = toNumber(value);
  if (numericValue === undefined) {
    return 0;
  }

  return Math.abs(numericValue) <= 1 ? numericValue * 100 : numericValue;
}

function round(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function normalizeWeightPercent(rawWeight: number): number {
  return rawWeight <= 1 ? rawWeight * 100 : rawWeight;
}

function normalizeSymbols(symbols: string[]): string[] {
  const unique = new Set<string>();
  for (const symbol of symbols) {
    const normalized = symbol.trim().toUpperCase();
    if (normalized) {
      unique.add(normalized);
    }
  }

  return Array.from(unique);
}

function formatDateParam(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function resolveBacktestDateRange(startDate?: string, endDate?: string): { start: Date; end: Date } {
  const parsedEnd = endDate ? new Date(endDate) : new Date();
  const safeEnd = Number.isNaN(parsedEnd.getTime()) ? new Date() : parsedEnd;

  const parsedStart = startDate ? new Date(startDate) : null;
  const fallbackStart = new Date(safeEnd);
  fallbackStart.setDate(safeEnd.getDate() - DEFAULT_BACKTEST_DAYS);

  const safeStart = parsedStart && !Number.isNaN(parsedStart.getTime()) ? parsedStart : fallbackStart;

  if (safeStart.getTime() >= safeEnd.getTime()) {
    return {
      start: fallbackStart,
      end: safeEnd,
    };
  }

  return {
    start: safeStart,
    end: safeEnd,
  };
}

function normalizeDatedClosePoints(points: DatedClosePoint[]): DatedClosePoint[] {
  return points
    .filter((point) => Number.isFinite(point.close) && point.close > 0 && Boolean(point.date))
    .sort((left, right) => left.date.localeCompare(right.date));
}

function normalizeWeightMap(weightsMap: Record<string, number>): Record<string, number> {
  const entries = Object.entries(weightsMap)
    .map(([symbol, weight]) => [symbol.toUpperCase(), typeof weight === "number" ? weight : 0] as const)
    .filter(([, weight]) => Number.isFinite(weight) && weight > 0);

  const total = entries.reduce((sum, [, weight]) => sum + weight, 0);
  if (total <= 0) {
    return {};
  }

  return Object.fromEntries(entries.map(([symbol, weight]) => [symbol, weight / total]));
}

function rangeInMonths(start: Date, end: Date): number {
  const diffMs = Math.max(0, end.getTime() - start.getTime());
  const diffDays = Math.max(1, Math.ceil(diffMs / (24 * 60 * 60 * 1000)));
  return Math.max(1, Math.ceil(diffDays / 30));
}

function buildBacktestSeriesFromHistory(
  normalizedWeights: Record<string, number>,
  priceSeriesBySymbol: Record<string, DatedClosePoint[]>,
  benchmarkSeries: DatedClosePoint[],
): OptimizeResultData["backtest"] {
  if (benchmarkSeries.length < 2) {
    return [];
  }

  const benchmarkBase = benchmarkSeries[0].close;
  if (!Number.isFinite(benchmarkBase) || benchmarkBase <= 0) {
    return [];
  }

  const symbols = Object.keys(normalizedWeights)
    .filter((symbol) => (priceSeriesBySymbol[symbol] ?? []).length > 0);

  if (symbols.length === 0) {
    return [];
  }

  const cursors = Object.fromEntries(symbols.map((symbol) => [symbol, 0]));
  const latestCloseBySymbol = Object.fromEntries(symbols.map((symbol) => [symbol, null as number | null]));
  const baseCloseBySymbol = Object.fromEntries(symbols.map((symbol) => [symbol, priceSeriesBySymbol[symbol][0].close]));

  const rows = benchmarkSeries.flatMap((benchmarkPoint) => {
    let weightedRatio = 0;
    let usedWeight = 0;

    for (const symbol of symbols) {
      const series = priceSeriesBySymbol[symbol];
      let cursor = cursors[symbol] as number;

      while (cursor < series.length && series[cursor].date <= benchmarkPoint.date) {
        latestCloseBySymbol[symbol] = series[cursor].close;
        cursor += 1;
      }

      cursors[symbol] = cursor;

      const latestClose = latestCloseBySymbol[symbol];
      const baseClose = baseCloseBySymbol[symbol] as number;
      if (
        typeof latestClose !== "number"
        || !Number.isFinite(latestClose)
        || latestClose <= 0
        || !Number.isFinite(baseClose)
        || baseClose <= 0
      ) {
        continue;
      }

      const weight = normalizedWeights[symbol] ?? 0;
      weightedRatio += weight * (latestClose / baseClose);
      usedWeight += weight;
    }

    if (usedWeight <= 0 || !Number.isFinite(benchmarkPoint.close) || benchmarkPoint.close <= 0) {
      return [];
    }

    return [{
      portfolio: round((weightedRatio / usedWeight) * 100, 2),
      benchmark: round((benchmarkPoint.close / benchmarkBase) * 100, 2),
    }];
  });

  return rows.map((point, index) => ({
    day: index + 1,
    portfolio: point.portfolio,
    benchmark: point.benchmark,
  }));
}

async function fetchSymbolHistory(symbol: string, start: Date, end: Date): Promise<DatedClosePoint[]> {
  const response = await apiGet<BackendStockPriceResponse>(
    `/api/v1/market/stock/${encodeURIComponent(symbol)}/price?start_date=${formatDateParam(start)}&end_date=${formatDateParam(end)}`,
  );

  const rawPrices = response.data?.prices ?? [];
  const normalized = rawPrices.flatMap((point) => (
    typeof point.close === "number" && Number.isFinite(point.close) && point.date
      ? [{ date: point.date, close: point.close } satisfies DatedClosePoint]
      : []
  ));

  return normalizeDatedClosePoints(normalized);
}

async function fetchBenchmarkHistory(start: Date, end: Date): Promise<DatedClosePoint[]> {
  const months = rangeInMonths(start, end);
  const response = await apiGet<BackendIndexHistoryPoint[]>(
    `/api/v1/market/indices/history?symbols=${BACKTEST_BENCHMARK_SYMBOL}&months=${months}`,
  );

  if (!Array.isArray(response)) {
    return [];
  }

  const normalized = response.flatMap((point) => (
    typeof point.close === "number" && Number.isFinite(point.close) && typeof point.time === "string"
      ? [{ date: point.time, close: point.close } satisfies DatedClosePoint]
      : []
  ));

  return normalizeDatedClosePoints(normalized);
}

async function buildFallbackBacktest(
  weightsMap: Record<string, number>,
  startDate?: string,
  endDate?: string,
): Promise<OptimizeResultData["backtest"]> {
  const normalizedWeights = normalizeWeightMap(weightsMap);
  const symbols = Object.keys(normalizedWeights);

  if (symbols.length === 0) {
    return [];
  }

  const { start, end } = resolveBacktestDateRange(startDate, endDate);

  const benchmarkSeries = await fetchBenchmarkHistory(start, end).catch(() => [] as DatedClosePoint[]);
  if (benchmarkSeries.length === 0) {
    return [];
  }

  const symbolSeriesEntries = await Promise.all(
    symbols.map(async (symbol) => {
      const series = await fetchSymbolHistory(symbol, start, end).catch(() => [] as DatedClosePoint[]);
      return [symbol, series] as const;
    }),
  );

  return buildBacktestSeriesFromHistory(normalizedWeights, Object.fromEntries(symbolSeriesEntries), benchmarkSeries);
}

function getExtraMetric(extraData: Record<string, unknown> | null | undefined, keys: string[]): number | undefined {
  if (!extraData) {
    return undefined;
  }

  for (const key of keys) {
    const value = extraData[key];
    const numericValue = toNumber(value as number | string | null | undefined);
    if (numericValue !== undefined) {
      return numericValue;
    }
  }

  return undefined;
}

function getExtraNumberMap(
  extraData: Record<string, unknown> | null | undefined,
  keys: string[],
): Record<string, number> | undefined {
  if (!extraData) {
    return undefined;
  }

  for (const key of keys) {
    const value = extraData[key];
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      continue;
    }

    const numericEntries = Object.entries(value).flatMap(([entryKey, entryValue]) => (
      typeof entryValue === "number" && Number.isFinite(entryValue)
        ? [[entryKey, entryValue] as const]
        : []
    ));

    if (numericEntries.length > 0) {
      return Object.fromEntries(numericEntries);
    }
  }

  return undefined;
}

function buildResultFromWeights(
  weightsMap: Record<string, number>,
  sharesMap: Record<string, number>,
  budget: number,
  metrics: OptimizeMetricSummary,
  backtest: OptimizeResultData["backtest"] = [],
  allocationAmountsMap?: Record<string, number>,
): OptimizeResultData {
  const weights: OptimizeWeight[] = Object.entries(weightsMap)
    .map(([symbol, weight]) => ({
      symbol,
      weight: round(normalizeWeightPercent(weight), 2),
    }))
    .sort((left, right) => right.weight - left.weight);

  const allocation = weights.map((item) => {
    const actualAmount = allocationAmountsMap?.[item.symbol];
    const amount = typeof actualAmount === "number" && Number.isFinite(actualAmount)
      ? Math.round(actualAmount)
      : Math.round((budget * item.weight) / 100);

    return {
      symbol: item.symbol,
      weight: item.weight,
      shares: Math.max(0, Math.round(sharesMap[item.symbol] ?? 0)),
      amount,
    };
  });

  return {
    weights,
    allocation,
    metrics,
    backtest,
  };
}

function buildMockResult(payload: CalculateOptimizationPayload): OptimizeResultData {
  const symbols = normalizeSymbols(payload.stocks);
  const fallbackSymbols = symbols.length > 0 ? symbols : optimizeMock.result.weights.map((item) => item.symbol);
  const budget = payload.budget > 0 ? payload.budget : 100_000_000;

  const seedWeights = [0.34, 0.24, 0.18, 0.14, 0.1, 0.08, 0.06];
  const selectedWeights = fallbackSymbols.map((_, index) => seedWeights[index] ?? 0.05);
  const totalWeight = selectedWeights.reduce((sum, value) => sum + value, 0);
  const normalizedWeightMap = Object.fromEntries(
    fallbackSymbols.map((symbol, index) => [symbol, selectedWeights[index] / totalWeight]),
  ) as Record<string, number>;

  const mockSharesMap = Object.fromEntries(
    fallbackSymbols.map((symbol, index) => {
      const amount = budget * normalizedWeightMap[symbol];
      const fakePrice = 25000 + index * 8000;
      return [symbol, Math.max(1, Math.floor(amount / fakePrice))];
    }),
  ) as Record<string, number>;

  return buildResultFromWeights(
    normalizedWeightMap,
    mockSharesMap,
    budget,
    optimizeMock.result.metrics,
    optimizeMock.result.backtest,
  );
}

export async function getOptimizationAlgorithms(): Promise<OptimizeAlgorithm[]> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return optimizeMock.algorithms;
  }

  const response = await apiAuthGet<BackendOptimizationModelsResponse>("/api/v1/optimize/models");
  const models = response.models ?? [];

  return models.map(localizeAlgorithmModel);
}

export async function searchOptimizationStocks(query: string, limit = 8): Promise<OptimizeStockSearchItem[]> {
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
        || (stock.name ?? "").toLowerCase().includes(lowerQuery),
      )
      .slice(0, limit);
  }

  const result = await apiGet<BackendStockSearchResult[]>(
    `/api/v1/market/search?query=${encodeURIComponent(normalized)}&limit=${limit}`,
  );

  return result.map((item) => ({
    symbol: item.symbol,
    name: item.name,
    exchange: item.exchange,
  }));
}

export async function calculateOptimization(payload: CalculateOptimizationPayload): Promise<OptimizeResultData> {
  const symbols = normalizeSymbols(payload.stocks);

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return buildMockResult(payload);
  }

  const response = await apiAuthPost<BackendOptimizationRunResponse>("/api/v1/optimize/run", {
    symbols,
    model: payload.algorithm,
    investment: payload.budget,
    constraints: payload.constraints,
    portfolio_id: payload.portfolioId,
    start_date: payload.startDate,
    end_date: payload.endDate,
  });

  const maxDrawdownRaw = getExtraMetric(response.extra_data, ["max_drawdown", "maxDrawdown", "Maximum Drawdown"]);
  const cvarRaw = getExtraMetric(response.extra_data, ["cvar", "CVaR", "conditional_var", "Rủi ro CVaR"]);
  const cdarRaw = getExtraMetric(response.extra_data, ["cdar", "CDaR", "conditional_drawdown", "Rủi ro CDaR"]);
  const betaRaw = getExtraMetric(response.extra_data, ["beta", "Beta"]);
  const expectedReturnRaw = toNumber(response.expected_return)
    ?? toNumber(response.expectedReturn)
    ?? getExtraMetric(response.extra_data, ["expected_return", "expectedReturn", "Lợi nhuận kỳ vọng"]);
  const volatilityRaw = toNumber(response.risk_volatility)
    ?? toNumber(response.expected_volatility)
    ?? toNumber(response.riskVolatility)
    ?? toNumber(response.volatility)
    ?? getExtraMetric(response.extra_data, [
      "risk_volatility",
      "expected_volatility",
      "riskVolatility",
      "expectedVolatility",
      "volatility",
      "Rủi ro (Độ lệch chuẩn)",
    ]);
  const sharpeRaw = toNumber(response.sharpe_ratio)
    ?? toNumber(response.sharpeRatio)
    ?? getExtraMetric(response.extra_data, ["sharpe_ratio", "sharpeRatio", "Tỷ lệ Sharpe"]);
  const allocationAmounts = getExtraNumberMap(response.extra_data, ["allocation_amounts", "allocationAmounts"]);
  const latestPrices = getExtraNumberMap(response.extra_data, ["latest_prices", "latestPrices", "Giá mã cổ phiếu"]);

  const normalizedAllocationAmounts = allocationAmounts ?? (latestPrices
    ? Object.fromEntries(
      Object.entries(response.shares ?? {}).flatMap(([symbol, shares]) => {
        const price = latestPrices[symbol];
        if (typeof shares !== "number" || !Number.isFinite(shares) || typeof price !== "number") {
          return [];
        }

        return [[symbol, shares * price] as const];
      }),
    )
    : undefined);

  const metrics: OptimizeMetricSummary = {
    expectedReturn: round(toPercentValue(expectedReturnRaw), 4),
    volatility: round(toPercentValue(volatilityRaw), 4),
    sharpeRatio: round(sharpeRaw ?? 0, 2),
    maxDrawdown: maxDrawdownRaw !== undefined ? round(toPercentValue(maxDrawdownRaw), 2) : undefined,
    cvar: cvarRaw !== undefined ? round(toPercentValue(cvarRaw), 2) : undefined,
    cdar: cdarRaw !== undefined ? round(toPercentValue(cdarRaw), 2) : undefined,
    beta: betaRaw !== undefined ? round(betaRaw, 2) : undefined,
  };
  const backtest = await buildFallbackBacktest(response.weights ?? {}, payload.startDate, payload.endDate);

  return buildResultFromWeights(
    response.weights ?? {},
    response.shares ?? {},
    payload.budget,
    metrics,
    backtest,
    normalizedAllocationAmounts,
  );
}

export async function getOptimizeData(): Promise<OptimizePageData> {
  const [algorithms] = await Promise.all([getOptimizationAlgorithms()]);
  return {
    algorithms,
    result: optimizeMock.result,
  };
}
