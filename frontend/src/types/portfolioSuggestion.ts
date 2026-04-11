export interface StockCatalogItem {
  symbol: string;
  name: string;
  exchange: string;
  sector: string;
  price: number;
  priceChangePercent: number;
  weeklyChangePercent: number;
  weeklyVolume: number;
  marketCap: number;
  isForeignNetBuy: boolean;
  isHighDividend: boolean;
  isNewlyListed: boolean;
}

export interface StockSuggestionGroup {
  id: string;
  title: string;
  description: string;
  stocks: StockCatalogItem[];
}

export interface SystemPortfolioPreset {
  id: string;
  name: string;
  criteria: string;
  description: string;
  stocks: StockCatalogItem[];
}

export interface PortfolioSuggestionData {
  catalog: StockCatalogItem[];
  trending: StockSuggestionGroup[];
  presets: SystemPortfolioPreset[];
}
