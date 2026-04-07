import { isMockMode, MOCK_DELAY_MS } from "@/config/env";
import { apiAuthGet } from "@/lib/apiAuth";
import { portfolioMock } from "@/mocks/portfolio.mock";
import type {
  PortfolioHolding,
  PortfolioItem,
  PortfolioListData,
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
  total_investment: number;
  stocks: BackendPortfolioStock[];
}

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function mapPortfolio(portfolio: BackendPortfolio): PortfolioItem {
  const totalInvested = Number(portfolio.total_investment);

  const holdings: PortfolioHolding[] = portfolio.stocks.map((stock) => {
    const positionValue = stock.purchase_price * stock.shares;

    return {
      symbol: stock.symbol,
      shares: stock.shares,
      avgPrice: stock.purchase_price,
      currentPrice: stock.purchase_price,
      weight: totalInvested > 0 ? Math.round((positionValue / totalInvested) * 100) : 0,
    };
  });

  return {
    id: String(portfolio.id),
    name: portfolio.name,
    totalInvested,
    currentValue: totalInvested,
    pnl: 0,
    pnlPercent: 0,
    holdings,
  };
}

export async function getPortfolioList(): Promise<PortfolioListData> {
  if (isMockMode) {
    await delay(MOCK_DELAY_MS);
    return portfolioMock;
  }

  const raw = await apiAuthGet<BackendPortfolio[]>("/api/v1/portfolios");
  return { portfolios: raw.map(mapPortfolio) };
}
