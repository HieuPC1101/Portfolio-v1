import { describe, expect, it } from "vitest";
import {
  enrichCatalogItemWithLiveQuote,
  getPortfolioSuggestionData,
  getSimilarStocks,
} from "@/repositories/portfolioSuggestionRepository";
import type { StockCatalogItem } from "@/types/portfolioSuggestion";

describe("portfolioSuggestionRepository", () => {
  it("trả về đủ 5 danh mục hệ thống", async () => {
    const data = await getPortfolioSuggestionData();

    expect(data.catalog.length).toBeGreaterThan(10);
    expect(data.trending.length).toBeGreaterThan(0);
    expect(data.presets.map((preset) => preset.name)).toEqual([
      "Tăng trưởng",
      "Sôi động",
      "Khối ngoại quan tâm",
      "Cổ tức cao",
      "Mới lên sàn",
    ]);
  });

  it("gợi ý mã tương tự theo ngành hoặc sàn", async () => {
    const similar = await getSimilarStocks("VCB", 5);

    expect(similar.length).toBeGreaterThan(0);
    expect(similar.some((item) => item.symbol !== "VCB")).toBe(true);
  });

  it("trả mảng rỗng khi mã nguồn không tồn tại", async () => {
    const similar = await getSimilarStocks("ZZZZ", 5);
    expect(similar).toEqual([]);
  });

  it("vẫn cập nhật giá khi overview lỗi", async () => {
    const stock: StockCatalogItem = {
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
    };

    const updated = await enrichCatalogItemWithLiveQuote(
      stock,
      async () => [
        { date: "2026-04-07", open: 129.8, high: 130.1, low: 128.9, close: 129.5, volume: 1_500_000 },
        { date: "2026-04-08", open: 130.1, high: 131.2, low: 129.7, close: 130.5, volume: 1_300_000 },
        { date: "2026-04-09", open: 131.2, high: 132.0, low: 130.8, close: 131.7, volume: 1_400_000 },
        { date: "2026-04-10", open: 132.0, high: 132.5, low: 131.1, close: 132.3, volume: 1_600_000 },
      ],
      async () => {
        throw new Error("overview unavailable");
      },
    );

    expect(updated.price).toBe(132300);
    expect(updated.priceChangePercent).toBeCloseTo(0.46, 2);
    expect(updated.weeklyChangePercent).toBeCloseTo(2.16, 2);
    expect(updated.weeklyVolume).toBe(5_800_000);
    expect(updated.marketCap).toBe(stock.marketCap);
  });
});
