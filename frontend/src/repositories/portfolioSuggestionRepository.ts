import type {
  PortfolioSuggestionData,
  StockCatalogItem,
  StockSuggestionGroup,
  SystemPortfolioPreset,
} from "@/types/portfolioSuggestion";

const STOCK_CATALOG: StockCatalogItem[] = [
  {
    symbol: "VCB",
    name: "Vietcombank",
    exchange: "HOSE",
    sector: "Ngân hàng",
    price: 85400,
    priceChangePercent: 1.43,
    weeklyChangePercent: 2.5,
    weeklyVolume: 8_900_000,
    marketCap: 480_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "FPT",
    name: "FPT Corporation",
    exchange: "HOSE",
    sector: "Công nghệ",
    price: 132500,
    priceChangePercent: 2.71,
    weeklyChangePercent: 4.2,
    weeklyVolume: 12_400_000,
    marketCap: 190_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "VNM",
    name: "Vinamilk",
    exchange: "HOSE",
    sector: "Thực phẩm",
    price: 72600,
    priceChangePercent: -0.55,
    weeklyChangePercent: -0.8,
    weeklyVolume: 7_200_000,
    marketCap: 152_000,
    isForeignNetBuy: false,
    isHighDividend: true,
    isNewlyListed: false,
  },
  {
    symbol: "HPG",
    name: "Hòa Phát",
    exchange: "HOSE",
    sector: "Thép",
    price: 28350,
    priceChangePercent: -2.91,
    weeklyChangePercent: -4.1,
    weeklyVolume: 31_500_000,
    marketCap: 165_000,
    isForeignNetBuy: false,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "MWG",
    name: "Thế Giới Di Động",
    exchange: "HOSE",
    sector: "Bán lẻ",
    price: 56200,
    priceChangePercent: 3.31,
    weeklyChangePercent: 6.4,
    weeklyVolume: 22_600_000,
    marketCap: 83_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "MBB",
    name: "MB Bank",
    exchange: "HOSE",
    sector: "Ngân hàng",
    price: 26200,
    priceChangePercent: 1.8,
    weeklyChangePercent: 3.2,
    weeklyVolume: 26_000_000,
    marketCap: 118_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "TCB",
    name: "Techcombank",
    exchange: "HOSE",
    sector: "Ngân hàng",
    price: 35700,
    priceChangePercent: 2,
    weeklyChangePercent: 3.8,
    weeklyVolume: 19_800_000,
    marketCap: 126_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "ACB",
    name: "ACB",
    exchange: "HOSE",
    sector: "Ngân hàng",
    price: 25100,
    priceChangePercent: 0.95,
    weeklyChangePercent: 1.6,
    weeklyVolume: 14_500_000,
    marketCap: 95_000,
    isForeignNetBuy: false,
    isHighDividend: true,
    isNewlyListed: false,
  },
  {
    symbol: "SSI",
    name: "Chứng khoán SSI",
    exchange: "HOSE",
    sector: "Tài chính",
    price: 32100,
    priceChangePercent: -2.13,
    weeklyChangePercent: -3.5,
    weeklyVolume: 28_700_000,
    marketCap: 49_000,
    isForeignNetBuy: false,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "VRE",
    name: "Vincom Retail",
    exchange: "HOSE",
    sector: "Bất động sản",
    price: 28900,
    priceChangePercent: -1.7,
    weeklyChangePercent: -2.9,
    weeklyVolume: 15_000_000,
    marketCap: 65_000,
    isForeignNetBuy: false,
    isHighDividend: true,
    isNewlyListed: false,
  },
  {
    symbol: "VIC",
    name: "Vingroup",
    exchange: "HOSE",
    sector: "Bất động sản",
    price: 42700,
    priceChangePercent: 1.1,
    weeklyChangePercent: 2.4,
    weeklyVolume: 11_200_000,
    marketCap: 162_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "VHM",
    name: "Vinhomes",
    exchange: "HOSE",
    sector: "Bất động sản",
    price: 40950,
    priceChangePercent: 0.8,
    weeklyChangePercent: 1.7,
    weeklyVolume: 9_400_000,
    marketCap: 178_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "DGC",
    name: "Hóa chất Đức Giang",
    exchange: "HOSE",
    sector: "Hóa chất",
    price: 112400,
    priceChangePercent: 2.3,
    weeklyChangePercent: 4.7,
    weeklyVolume: 4_100_000,
    marketCap: 42_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "PVS",
    name: "DVKT Dầu khí",
    exchange: "HNX",
    sector: "Dầu khí",
    price: 34900,
    priceChangePercent: 1.9,
    weeklyChangePercent: 3.1,
    weeklyVolume: 10_400_000,
    marketCap: 16_000,
    isForeignNetBuy: true,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "CEO",
    name: "CEO Group",
    exchange: "HNX",
    sector: "Bất động sản",
    price: 17800,
    priceChangePercent: 0.4,
    weeklyChangePercent: 1.2,
    weeklyVolume: 7_600_000,
    marketCap: 9_800,
    isForeignNetBuy: false,
    isHighDividend: false,
    isNewlyListed: false,
  },
  {
    symbol: "BSR",
    name: "Lọc hóa dầu Bình Sơn",
    exchange: "UPCOM",
    sector: "Dầu khí",
    price: 21900,
    priceChangePercent: 1.2,
    weeklyChangePercent: 2,
    weeklyVolume: 12_900_000,
    marketCap: 68_000,
    isForeignNetBuy: true,
    isHighDividend: true,
    isNewlyListed: false,
  },
  {
    symbol: "QNS",
    name: "Đường Quảng Ngãi",
    exchange: "UPCOM",
    sector: "Thực phẩm",
    price: 48500,
    priceChangePercent: 0.6,
    weeklyChangePercent: 1.8,
    weeklyVolume: 2_800_000,
    marketCap: 17_000,
    isForeignNetBuy: false,
    isHighDividend: true,
    isNewlyListed: false,
  },
  {
    symbol: "VIX",
    name: "Chứng khoán VIX",
    exchange: "HOSE",
    sector: "Tài chính",
    price: 18100,
    priceChangePercent: 4.6,
    weeklyChangePercent: 8.3,
    weeklyVolume: 39_500_000,
    marketCap: 22_000,
    isForeignNetBuy: false,
    isHighDividend: false,
    isNewlyListed: true,
  },
  {
    symbol: "FTS",
    name: "Chứng khoán FPT",
    exchange: "HOSE",
    sector: "Tài chính",
    price: 53100,
    priceChangePercent: 1.6,
    weeklyChangePercent: 3.4,
    weeklyVolume: 6_300_000,
    marketCap: 16_500,
    isForeignNetBuy: true,
    isHighDividend: true,
    isNewlyListed: true,
  },
];

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function takeTopBy(
  items: StockCatalogItem[],
  sortFn: (left: StockCatalogItem, right: StockCatalogItem) => number,
  limit = 5,
): StockCatalogItem[] {
  return [...items].sort(sortFn).slice(0, limit);
}

function buildTrendingGroups(catalog: StockCatalogItem[]): StockSuggestionGroup[] {
  return [
    {
      id: "top-gainers",
      title: "Top tăng giá tuần",
      description: "Những mã tăng mạnh trong 1 tuần gần nhất",
      stocks: takeTopBy(catalog, (left, right) => right.weeklyChangePercent - left.weeklyChangePercent),
    },
    {
      id: "top-losers",
      title: "Top giảm giá tuần",
      description: "Danh sách giảm sâu để theo dõi cơ hội",
      stocks: takeTopBy(catalog, (left, right) => left.weeklyChangePercent - right.weeklyChangePercent),
    },
    {
      id: "high-liquidity",
      title: "Thanh khoản cao",
      description: "Các mã có khối lượng giao dịch vượt trội",
      stocks: takeTopBy(catalog, (left, right) => right.weeklyVolume - left.weeklyVolume),
    },
    {
      id: "foreign-net-buy",
      title: "Khối ngoại mua ròng",
      description: "Mã được khối ngoại mua ròng gần đây",
      stocks: takeTopBy(
        catalog.filter((item) => item.isForeignNetBuy),
        (left, right) => right.marketCap - left.marketCap,
      ),
    },
  ];
}

function buildPreset(id: string, name: string, description: string, criteria: string, stocks: StockCatalogItem[]): SystemPortfolioPreset {
  return {
    id,
    name,
    description,
    criteria,
    stocks,
  };
}

function buildPresets(catalog: StockCatalogItem[]): SystemPortfolioPreset[] {
  const growth = takeTopBy(
    catalog.filter((item) => item.weeklyChangePercent > 2),
    (left, right) => right.weeklyChangePercent - left.weeklyChangePercent,
  );

  const active = takeTopBy(catalog, (left, right) => right.weeklyVolume - left.weeklyVolume);

  const foreignFocus = takeTopBy(
    catalog.filter((item) => item.isForeignNetBuy),
    (left, right) => right.marketCap - left.marketCap,
  );

  const highDividend = takeTopBy(
    catalog.filter((item) => item.isHighDividend),
    (left, right) => right.marketCap - left.marketCap,
  );

  const newlyListed = takeTopBy(
    catalog.filter((item) => item.isNewlyListed),
    (left, right) => right.weeklyChangePercent - left.weeklyChangePercent,
  );

  return [
    buildPreset(
      "growth",
      "Tăng trưởng",
      "Ưu tiên mã có xu hướng tăng rõ rệt trong ngắn hạn.",
      "Top mã tăng giá tuần > 2% và thanh khoản ổn định.",
      growth,
    ),
    buildPreset(
      "active",
      "Sôi động",
      "Tập trung vào nhóm có dòng tiền giao dịch mạnh.",
      "Top khối lượng giao dịch 1 tuần.",
      active,
    ),
    buildPreset(
      "foreign",
      "Khối ngoại quan tâm",
      "Nhóm mã vốn hóa tốt được nhà đầu tư nước ngoài ưu tiên.",
      "Mã được đánh dấu mua ròng bởi khối ngoại.",
      foreignFocus,
    ),
    buildPreset(
      "dividend",
      "Cổ tức cao",
      "Danh mục phù hợp chiến lược thu nhập ổn định.",
      "Mã có lịch sử cổ tức tốt, thanh khoản ổn.",
      highDividend,
    ),
    buildPreset(
      "newly-listed",
      "Mới lên sàn",
      "Các mã mới niêm yết có biến động đáng chú ý.",
      "Mã mới niêm yết có đà giá tích cực.",
      newlyListed,
    ),
  ];
}

export async function getPortfolioSuggestionData(): Promise<PortfolioSuggestionData> {
  const catalog = clone(STOCK_CATALOG);

  return {
    catalog,
    trending: buildTrendingGroups(catalog),
    presets: buildPresets(catalog),
  };
}

export async function getSimilarStocks(symbol: string, limit = 5): Promise<StockCatalogItem[]> {
  const normalizedSymbol = symbol.trim().toUpperCase();
  if (!normalizedSymbol) {
    return [];
  }

  const catalog = clone(STOCK_CATALOG);
  const source = catalog.find((item) => item.symbol === normalizedSymbol);
  if (!source) {
    return [];
  }

  return catalog
    .filter((item) => item.symbol !== source.symbol)
    .filter((item) => item.sector === source.sector || item.exchange === source.exchange)
    .sort((left, right) => {
      const sectorScore = Number(right.sector === source.sector) - Number(left.sector === source.sector);
      if (sectorScore !== 0) {
        return sectorScore;
      }

      const marketCapDistanceLeft = Math.abs(left.marketCap - source.marketCap);
      const marketCapDistanceRight = Math.abs(right.marketCap - source.marketCap);
      return marketCapDistanceLeft - marketCapDistanceRight;
    })
    .slice(0, limit);
}
