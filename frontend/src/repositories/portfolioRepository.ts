import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiAuthDelete, apiAuthGet, apiAuthPost, apiAuthPut } from "@/lib/apiAuth";
import { portfolioMock } from "@/mocks/portfolio.mock";
import { getStockOHLC } from "@/repositories/marketRepository";
import type {
  AddStockPayload,
  CreatePortfolioPayload,
  PortfolioHolding,
  PortfolioItem,
  PortfolioListData,
  UpdatePortfolioPayload,
  UpdateStockPayload,
} from "@/types/portfolio";

interface BackendPortfolioStock {
  id: number;
  symbol: string;
  shares: number;
  purchase_price: number;
}

interface BackendPortfolio {
  id: number;
  name: string;
   description?: string | null;
  total_investment: number;
  stocks: BackendPortfolioStock[];
}

interface BackendPortfolioCreatePayload {
  name: string;
  description?: string;
  total_investment: number;
}

interface BackendPortfolioUpdatePayload {
  name?: string;
  description?: string;
  total_investment?: number;
}

interface BackendPortfolioStockPayload {
  symbol: string;
  shares: number;
  purchase_price: number;
}

interface BackendPortfolioStockUpdatePayload {
  shares?: number;
  purchase_price?: number;
}

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

let mockState: PortfolioListData = clone(portfolioMock);

function recalculatePortfolio(portfolio: PortfolioItem): PortfolioItem {
  const totalInvested = Number(portfolio.totalInvested ?? 0);
  const holdingsCostBasis = portfolio.holdings.reduce((sum, holding) => sum + holding.avgPrice * holding.shares, 0);
  const holdingsCurrentValue = portfolio.holdings.reduce((sum, holding) => sum + holding.currentPrice * holding.shares, 0);
  const remainingBudget = totalInvested - holdingsCostBasis;
  const currentValue = holdingsCurrentValue + remainingBudget;
  const pnl = currentValue - totalInvested;
  const pnlPercent = totalInvested > 0 ? (pnl / totalInvested) * 100 : 0;

  const holdings = portfolio.holdings.map((holding) => {
    const positionValue = holding.currentPrice * holding.shares;
    const weight = holdingsCurrentValue > 0 ? Math.round((positionValue / holdingsCurrentValue) * 100) : 0;
    return { ...holding, weight };
  });

  return {
    ...portfolio,
    totalInvested,
    currentValue,
    pnl,
    pnlPercent,
    holdings,
  };
}

function toBackendCreatePayload(payload: CreatePortfolioPayload): BackendPortfolioCreatePayload {
  return {
    name: payload.name,
    description: payload.description,
    total_investment: payload.totalInvestment,
  };
}

function toBackendUpdatePayload(payload: UpdatePortfolioPayload): BackendPortfolioUpdatePayload {
  return {
    name: payload.name,
    description: payload.description,
    total_investment: payload.totalInvestment,
  };
}

function toBackendStockPayload(payload: AddStockPayload): BackendPortfolioStockPayload {
  return {
    symbol: payload.symbol,
    shares: payload.shares,
    purchase_price: payload.purchasePrice,
  };
}

function toBackendStockUpdatePayload(payload: UpdateStockPayload): BackendPortfolioStockUpdatePayload {
  return {
    shares: payload.shares,
    purchase_price: payload.purchasePrice,
  };
}

function mapHolding(stock: BackendPortfolioStock, totalInvested: number): PortfolioHolding {
  const avgPrice = Number(stock.purchase_price ?? 0);
  const shares = Number(stock.shares ?? 0);
  const positionValue = avgPrice * shares;

  return {
    id: String(stock.id),
    symbol: stock.symbol,
    shares,
    avgPrice,
    currentPrice: avgPrice,
    weight: totalInvested > 0 ? Math.round((positionValue / totalInvested) * 100) : 0,
  };
}

function mapPortfolio(portfolio: BackendPortfolio): PortfolioItem {
  const investedFromHoldings = portfolio.stocks.reduce(
    (sum, stock) => sum + Number(stock.purchase_price ?? 0) * Number(stock.shares ?? 0),
    0,
  );

  const configuredInvestment = Number(portfolio.total_investment ?? 0);
  const totalInvested = configuredInvestment > 0
    ? configuredInvestment
    : investedFromHoldings;
  const holdingWeightBase = investedFromHoldings > 0
    ? investedFromHoldings
    : totalInvested;

  const holdings: PortfolioHolding[] = portfolio.stocks.map((stock) => mapHolding(stock, holdingWeightBase));

  return {
    id: String(portfolio.id),
    name: portfolio.name,
    description: portfolio.description ?? undefined,
    totalInvested,
    currentValue: totalInvested,
    pnl: 0,
    pnlPercent: 0,
    holdings,
  };
}

function toVndFromLatestClose(latestClose: number): number | null {
  if (!Number.isFinite(latestClose) || latestClose <= 0) {
    return null;
  }

  return Math.round(latestClose * 1_000);
}

async function buildLivePriceMap(portfolios: PortfolioItem[]): Promise<Map<string, number>> {
  const symbols = Array.from(new Set(
    portfolios.flatMap((portfolio) => portfolio.holdings.map((holding) => holding.symbol.trim().toUpperCase())),
  )).filter((symbol) => symbol.length > 0);

  if (symbols.length === 0) {
    return new Map();
  }

  const latestPrices = await Promise.all(symbols.map(async (symbol) => {
    try {
      const ohlc = await getStockOHLC(symbol, 2);
      const latestClose = ohlc[ohlc.length - 1]?.close;
      const currentPrice = typeof latestClose === "number" ? toVndFromLatestClose(latestClose) : null;

      return currentPrice && currentPrice > 0
        ? [symbol, currentPrice] as const
        : null;
    } catch {
      return null;
    }
  }));

  return new Map(latestPrices.filter((entry): entry is readonly [string, number] => entry !== null));
}

function applyLivePricesToPortfolio(portfolio: PortfolioItem, priceMap: Map<string, number>): PortfolioItem {
  const holdings = portfolio.holdings.map((holding) => {
    const symbol = holding.symbol.trim().toUpperCase();
    const livePrice = priceMap.get(symbol);

    if (typeof livePrice !== "number" || !Number.isFinite(livePrice) || livePrice <= 0) {
      return holding;
    }

    return {
      ...holding,
      currentPrice: livePrice,
    };
  });

  const holdingsCostBasis = holdings.reduce((sum, holding) => sum + holding.avgPrice * holding.shares, 0);
  const holdingsCurrentValue = holdings.reduce((sum, holding) => sum + holding.currentPrice * holding.shares, 0);
  const remainingBudget = portfolio.totalInvested - holdingsCostBasis;
  const currentValue = holdingsCurrentValue + remainingBudget;
  const pnl = currentValue - portfolio.totalInvested;
  const pnlPercent = portfolio.totalInvested > 0 ? (pnl / portfolio.totalInvested) * 100 : 0;

  const weightedHoldings = holdings.map((holding) => {
    const positionValue = holding.currentPrice * holding.shares;
    const weight = holdingsCurrentValue > 0 ? Math.round((positionValue / holdingsCurrentValue) * 100) : 0;
    return { ...holding, weight };
  });

  return {
    ...portfolio,
    currentValue,
    pnl,
    pnlPercent,
    holdings: weightedHoldings,
  };
}

export async function getPortfolioList(): Promise<PortfolioListData> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return clone(mockState);
  }

  const raw = await apiAuthGet<BackendPortfolio[]>("/api/v1/portfolios");
  const mapped = raw.map(mapPortfolio);
  const livePriceMap = await buildLivePriceMap(mapped);

  return {
    portfolios: mapped.map((portfolio) => applyLivePricesToPortfolio(portfolio, livePriceMap)),
  };
}

export async function createPortfolio(payload: CreatePortfolioPayload): Promise<PortfolioItem> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const nextId = String((Math.max(0, ...mockState.portfolios.map((item) => Number(item.id))) || 0) + 1);
    const created = recalculatePortfolio({
      id: nextId,
      name: payload.name,
      description: payload.description,
      totalInvested: payload.totalInvestment,
      currentValue: payload.totalInvestment,
      pnl: 0,
      pnlPercent: 0,
      holdings: [],
    });
    mockState = { portfolios: [created, ...mockState.portfolios] };
    return clone(created);
  }

  const created = await apiAuthPost<BackendPortfolio>("/api/v1/portfolios", toBackendCreatePayload(payload));
  return mapPortfolio(created);
}

export async function updatePortfolio(id: string, payload: UpdatePortfolioPayload): Promise<PortfolioItem> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const index = mockState.portfolios.findIndex((item) => item.id === id);
    if (index < 0) {
      throw new Error("Portfolio not found");
    }

    const current = mockState.portfolios[index];
    const updated = recalculatePortfolio({
      ...current,
      name: payload.name ?? current.name,
      description: payload.description ?? current.description,
      totalInvested: payload.totalInvestment ?? current.totalInvested,
    });

    const next = [...mockState.portfolios];
    next[index] = updated;
    mockState = { portfolios: next };
    return clone(updated);
  }

  const updated = await apiAuthPut<BackendPortfolio>(`/api/v1/portfolios/${id}`, toBackendUpdatePayload(payload));
  return mapPortfolio(updated);
}

export async function deletePortfolio(id: string): Promise<void> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    mockState = { portfolios: mockState.portfolios.filter((item) => item.id !== id) };
    return;
  }

  await apiAuthDelete(`/api/v1/portfolios/${id}`);
}

export async function addStockToPortfolio(portfolioId: string, payload: AddStockPayload): Promise<PortfolioHolding> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const index = mockState.portfolios.findIndex((item) => item.id === portfolioId);
    if (index < 0) {
      throw new Error("Portfolio not found");
    }

    const current = mockState.portfolios[index];
    const nextStockId = String((Math.max(0, ...current.holdings.map((holding) => Number(holding.id))) || 0) + 1);

    const updated = recalculatePortfolio({
      ...current,
      holdings: [
        ...current.holdings,
        {
          id: nextStockId,
          symbol: payload.symbol.toUpperCase(),
          shares: payload.shares,
          avgPrice: payload.purchasePrice,
          currentPrice: payload.purchasePrice,
          weight: 0,
        },
      ],
    });

    const next = [...mockState.portfolios];
    next[index] = updated;
    mockState = { portfolios: next };

    return clone(updated.holdings.find((holding) => holding.id === nextStockId)!);
  }

  const stock = await apiAuthPost<BackendPortfolioStock>(
    `/api/v1/portfolios/${portfolioId}/stocks`,
    toBackendStockPayload(payload),
  );
  return mapHolding(stock, stock.shares * Number(stock.purchase_price ?? 0));
}

export async function updatePortfolioStock(
  portfolioId: string,
  stockId: string,
  payload: UpdateStockPayload,
): Promise<PortfolioHolding> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const portfolioIndex = mockState.portfolios.findIndex((item) => item.id === portfolioId);
    if (portfolioIndex < 0) {
      throw new Error("Portfolio not found");
    }

    const current = mockState.portfolios[portfolioIndex];
    const holdingIndex = current.holdings.findIndex((holding) => holding.id === stockId);
    if (holdingIndex < 0) {
      throw new Error("Stock not found");
    }

    const nextHoldings = [...current.holdings];
    const holding = nextHoldings[holdingIndex];
    nextHoldings[holdingIndex] = {
      ...holding,
      shares: payload.shares ?? holding.shares,
      avgPrice: payload.purchasePrice ?? holding.avgPrice,
      currentPrice: payload.purchasePrice ?? holding.currentPrice,
    };

    const updated = recalculatePortfolio({ ...current, holdings: nextHoldings });
    const next = [...mockState.portfolios];
    next[portfolioIndex] = updated;
    mockState = { portfolios: next };

    return clone(updated.holdings[holdingIndex]);
  }

  const stock = await apiAuthPut<BackendPortfolioStock>(
    `/api/v1/portfolios/${portfolioId}/stocks/${stockId}`,
    toBackendStockUpdatePayload(payload),
  );
  return mapHolding(stock, stock.shares * Number(stock.purchase_price ?? 0));
}

export async function deletePortfolioStock(portfolioId: string, stockId: string): Promise<void> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const portfolioIndex = mockState.portfolios.findIndex((item) => item.id === portfolioId);
    if (portfolioIndex < 0) {
      throw new Error("Portfolio not found");
    }

    const current = mockState.portfolios[portfolioIndex];
    const updated = recalculatePortfolio({
      ...current,
      holdings: current.holdings.filter((holding) => holding.id !== stockId),
    });

    const next = [...mockState.portfolios];
    next[portfolioIndex] = updated;
    mockState = { portfolios: next };
    return;
  }

  await apiAuthDelete(`/api/v1/portfolios/${portfolioId}/stocks/${stockId}`);
}
