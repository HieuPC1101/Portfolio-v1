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
  expected_return: number | null;
  risk_volatility: number | null;
  sharpe_ratio: number | null;
  weights: Record<string, number>;
  shares: Record<string, number>;
  extra_data?: Record<string, unknown> | null;
}

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

function toPercentValue(value: number | null | undefined): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0;
  }

  return Math.abs(value) <= 1 ? value * 100 : value;
}

function round2(value: number): number {
  return Math.round(value * 100) / 100;
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

function getExtraMetric(extraData: Record<string, unknown> | null | undefined, keys: string[]): number | undefined {
  if (!extraData) {
    return undefined;
  }

  for (const key of keys) {
    const value = extraData[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
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
      weight: round2(normalizeWeightPercent(weight)),
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
    expectedReturn: round2(toPercentValue(response.expected_return)),
    volatility: round2(toPercentValue(response.risk_volatility)),
    sharpeRatio: round2(response.sharpe_ratio ?? 0),
    maxDrawdown: maxDrawdownRaw !== undefined ? round2(toPercentValue(maxDrawdownRaw)) : undefined,
    cvar: cvarRaw !== undefined ? round2(toPercentValue(cvarRaw)) : undefined,
    cdar: cdarRaw !== undefined ? round2(toPercentValue(cdarRaw)) : undefined,
    beta: betaRaw !== undefined ? round2(betaRaw) : undefined,
  };

  return buildResultFromWeights(
    response.weights ?? {},
    response.shares ?? {},
    payload.budget,
    metrics,
    [],
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
