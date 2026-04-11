export type FinancialPeriod = "quarterly" | "yearly";

export interface StockOverview {
  symbol: string;
  companyName: string | null;
  exchange: string | null;
  sector: string | null;
  industry: string | null;
  marketCap: number | null;
  sharesOutstanding: number | null;
  freeFloat: number | null;
  listingDate: string | null;
  headquarters: string | null;
  employeeCount: number | null;
  businessSummary: string | null;
  latestHighlights: string[];
  asOfDate: string | null;
  source: string | null;
}

export interface StockFinancialPeriod {
  periodLabel: string;
  year: number | null;
  quarter: number | null;
  revenue: number | null;
  grossProfit: number | null;
  operatingProfit: number | null;
  netIncome: number | null;
  totalAssets: number | null;
  totalLiabilities: number | null;
  equity: number | null;
  totalDebt: number | null;
  cashAndCashEquivalents: number | null;
  operatingCashFlow: number | null;
  investingCashFlow: number | null;
  financingCashFlow: number | null;
  freeCashFlow: number | null;
}

export interface StockFinancialsData {
  symbol: string;
  period: FinancialPeriod;
  items: StockFinancialPeriod[];
  asOfDate: string | null;
  source: string | null;
}

export interface StockRatios {
  symbol: string;
  pe: number | null;
  pb: number | null;
  evEbitda: number | null;
  grossMargin: number | null;
  netMargin: number | null;
  roe: number | null;
  roa: number | null;
  debtToEquity: number | null;
  asOfDate: string | null;
  reportingPeriod: string | null;
  qualityFlags: string[];
  source: string | null;
}
