import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

import {
  getFinancialPeriodLabel,
  getMarketData,
  getStockFinancials,
  getStockOverview,
  getStockRatios,
  getStockDetail,
  getStockPriceHistory,
  searchStocks,
} from "@/repositories/marketRepository";

describe("getMarketData", () => {
  it("trả về sectors và chuỗi foreign flow", async () => {
    const data = await getMarketData();

    expect(data.sectors.length).toBeGreaterThan(0);
    expect(data.foreignFlow.length).toBeGreaterThan(0);
    expect(data.topGainers[0]).toHaveProperty("symbol");
  });

  it("tìm kiếm mã cổ phiếu theo query", async () => {
    const results = await searchStocks("fpt");

    expect(results.length).toBeGreaterThan(0);
    expect(results[0].symbol).toBe("FPT");
  });

  it("trả mảng rỗng khi query trắng", async () => {
    const results = await searchStocks("   ");
    expect(results).toEqual([]);
  });

  it("trả dữ liệu chi tiết + lịch sử 30 phiên", async () => {
    const detail = await getStockDetail("FPT");

    expect(detail.symbol).toBe("FPT");
    expect(detail.history30d.length).toBe(30);
    expect(detail.ohlc30d.length).toBe(30);
    expect(detail.ohlc30d[0]).toHaveProperty("open");
    expect(detail.ohlc30d[0]).toHaveProperty("high");
    expect(detail.ohlc30d[0]).toHaveProperty("low");
    expect(detail.ohlc30d[0]).toHaveProperty("close");
    expect(detail.ohlc30d[0]).toHaveProperty("volume");
    expect(detail.price).toBeGreaterThan(0);
  });

  it("lấy lịch sử giá theo số phiên yêu cầu", async () => {
    const history = await getStockPriceHistory("MWG", 7);

    expect(history.length).toBe(7);
    expect(history[0]).toHaveProperty("date");
    expect(history[0]).toHaveProperty("close");
  });

  it("trả overview công ty với mock data", async () => {
    const overview = await getStockOverview("FPT");

    expect(overview.symbol).toBe("FPT");
    expect(overview.companyName).toBeTruthy();
    expect(Array.isArray(overview.latestHighlights)).toBe(true);
  });

  it("trả ratios có đủ nhóm chỉ số", async () => {
    const ratios = await getStockRatios("FPT");

    expect(ratios.symbol).toBe("FPT");
    expect(ratios).toHaveProperty("pe");
    expect(ratios).toHaveProperty("pb");
    expect(ratios).toHaveProperty("evEbitda");
    expect(ratios).toHaveProperty("grossMargin");
    expect(ratios).toHaveProperty("netMargin");
    expect(ratios).toHaveProperty("roe");
    expect(ratios).toHaveProperty("roa");
    expect(ratios).toHaveProperty("debtToEquity");
    expect(ratios).toHaveProperty("asOfDate");
    expect(ratios).toHaveProperty("reportingPeriod");
  });

  it("trả financials theo quý", async () => {
    const financials = await getStockFinancials("FPT", "quarterly", 4);

    expect(financials.symbol).toBe("FPT");
    expect(financials.period).toBe("quarterly");
    expect(financials.items.length).toBeGreaterThan(0);
    expect(financials.items[0]).toHaveProperty("periodLabel");
    expect(financials.items[0]).toHaveProperty("revenue");
  });

  it("format label kỳ báo cáo đúng", () => {
    expect(getFinancialPeriodLabel({ periodLabel: "Q4/2025", year: 2025, quarter: 4 } as any)).toBe("Q4/2025");
    expect(getFinancialPeriodLabel({ periodLabel: "", year: 2024, quarter: null } as any)).toBe("2024");
    expect(getFinancialPeriodLabel({ periodLabel: "", year: null, quarter: null } as any)).toBe("--");
  });
});
