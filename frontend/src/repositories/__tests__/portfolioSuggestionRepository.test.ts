import { describe, expect, it } from "vitest";
import {
  getPortfolioSuggestionData,
  getSimilarStocks,
} from "@/repositories/portfolioSuggestionRepository";

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
});
