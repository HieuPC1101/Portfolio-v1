import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import { apiAuthDelete, apiAuthGet, apiAuthPost, apiAuthPut } from "@/lib/apiAuth";
import { portfolioMock } from "@/mocks/portfolio.mock";
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
  const totalInvested = portfolio.holdings.reduce((sum, holding) => sum + holding.avgPrice * holding.shares, 0);
  const currentValue = portfolio.holdings.reduce((sum, holding) => sum + holding.currentPrice * holding.shares, 0);
  const pnl = currentValue - totalInvested;
  const pnlPercent = totalInvested > 0 ? (pnl / totalInvested) * 100 : 0;

  const holdings = portfolio.holdings.map((holding) => {
    const positionValue = holding.currentPrice * holding.shares;
    const weight = currentValue > 0 ? Math.round((positionValue / currentValue) * 100) : 0;
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
  const fallbackInvestment = portfolio.stocks.reduce(
    (sum, stock) => sum + Number(stock.purchase_price ?? 0) * Number(stock.shares ?? 0),
    0,
  );

  const totalInvested = Number(portfolio.total_investment ?? 0) > 0
    ? Number(portfolio.total_investment)
    : fallbackInvestment;

  const holdings: PortfolioHolding[] = portfolio.stocks.map((stock) => mapHolding(stock, totalInvested));

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

export async function getPortfolioList(): Promise<PortfolioListData> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return clone(mockState);
  }

  const raw = await apiAuthGet<BackendPortfolio[]>("/api/v1/portfolios");
  return { portfolios: raw.map(mapPortfolio) };
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
