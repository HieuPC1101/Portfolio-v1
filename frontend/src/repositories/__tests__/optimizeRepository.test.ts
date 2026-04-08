import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@/config/env", () => ({
  isMockMode: false,
  MOCK_DELAY_MS: 0,
  API_BASE_URL: "http://localhost:8000",
  AUTH_TOKEN: "test-token",
}));

import {
  calculateOptimization,
  getOptimizationAlgorithms,
  searchOptimizationStocks,
} from "@/repositories/optimizeRepository";

describe("optimizeRepository", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("lấy danh sách thuật toán từ backend", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          models: [
            {
              id: "max_sharpe",
              name: "Maximum Sharpe Ratio",
              description: "Maximize risk-adjusted returns",
            },
          ],
        }),
      }),
    );

    const algorithms = await getOptimizationAlgorithms();

    expect(algorithms).toEqual([
      {
        id: "max_sharpe",
        name: "Tối đa Sharpe Ratio",
        desc: "Tối ưu lợi nhuận điều chỉnh theo rủi ro",
      },
    ]);
    expect(fetch).toHaveBeenCalledWith("http://localhost:8000/api/v1/optimize/models", {
      method: "GET",
      headers: { Authorization: "Bearer test-token" },
      body: undefined,
    });
  });

  it("tìm kiếm cổ phiếu theo query", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue([
          {
            symbol: "FPT",
            name: "FPT Corporation",
            exchange: "HOSE",
            sector: "Technology",
          },
        ]),
      }),
    );

    const results = await searchOptimizationStocks("fpt", 8);

    expect(results).toHaveLength(1);
    expect(results[0].symbol).toBe("FPT");
    expect(fetch).toHaveBeenCalledWith(
      "http://localhost:8000/api/v1/market/search?query=fpt&limit=8",
    );
  });

  it("map kết quả tối ưu hóa từ backend sang dữ liệu hiển thị", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 201,
        json: vi.fn().mockResolvedValue({
          id: 12,
          user_id: 1,
          model_name: "max_sharpe",
          input_symbols: ["VCB", "FPT"],
          total_investment: 100000000,
          expected_return: 0.185,
          risk_volatility: 0.123,
          sharpe_ratio: 1.5,
          weights: {
            VCB: 0.65,
            FPT: 0.35,
          },
          shares: {
            VCB: 500,
            FPT: 320,
          },
          leftover_cash: 0,
          extra_data: {
            max_drawdown: -0.14,
            cvar: 0.08,
            cdar: 0.12,
            beta: 1.1,
            latest_prices: {
              VCB: 100000,
              FPT: 90000,
            },
            allocation_amounts: {
              VCB: 50000000,
              FPT: 28800000,
            },
          },
          created_at: "2026-04-08T01:00:00.000Z",
        }),
      }),
    );

    const result = await calculateOptimization({
      stocks: ["VCB", "FPT"],
      algorithm: "max_sharpe",
      budget: 100000000,
      constraints: {
        riskFreeRate: 0.05,
        targetReturn: null,
        maxWeight: 0.7,
        minWeight: 0.1,
      },
    });

    expect(result.metrics.expectedReturn).toBe(18.5);
    expect(result.metrics.volatility).toBe(12.3);
    expect(result.metrics.sharpeRatio).toBe(1.5);
    expect(result.metrics).toMatchObject({
      maxDrawdown: -14,
      cvar: 8,
      cdar: 12,
      beta: 1.1,
    });
    expect(result.weights).toEqual([
      { symbol: "VCB", weight: 65 },
      { symbol: "FPT", weight: 35 },
    ]);
    expect(result.allocation).toEqual([
      { symbol: "VCB", weight: 65, shares: 500, amount: 50000000 },
      { symbol: "FPT", weight: 35, shares: 320, amount: 28800000 },
    ]);
  });
});
