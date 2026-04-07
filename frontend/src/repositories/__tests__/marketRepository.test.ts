import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

import {
  getMarketData,
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
});
