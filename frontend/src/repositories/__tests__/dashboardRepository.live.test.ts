import { beforeEach, describe, expect, it, vi } from "vitest";
import { dashboardMock } from "@/mocks/dashboard.mock";

const { apiGetMock, apiAuthGetMock } = vi.hoisted(() => ({
  apiGetMock: vi.fn(),
  apiAuthGetMock: vi.fn(),
}));

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: false,
  MOCK_API_DELAY_MS: 0,
}));

vi.mock("@/lib/api", () => ({
  apiGet: apiGetMock,
}));

vi.mock("@/lib/apiAuth", () => ({
  apiAuthGet: apiAuthGetMock,
}));

import { getDashboardData } from "@/repositories/dashboardRepository";

function createMover(index: number) {
  return {
    ticker: `S${String(index + 1).padStart(2, "0")}`,
    price: 10000 + index * 100,
    daily_change: 10 - index * 0.5,
    avg_trading_value_20d: 2_000_000_000 - index * 10_000_000,
  };
}

describe("getDashboardData (live path)", () => {
  beforeEach(() => {
    apiGetMock.mockReset();
    apiAuthGetMock.mockReset();
  });

  it("giới hạn tối đa 10 cổ phiếu cho mỗi nhóm biến động nổi bật", async () => {
    const movers = Array.from({ length: 12 }, (_, index) => createMover(index));

    apiGetMock
      .mockResolvedValueOnce({
        vnindex: { name: "VN-Index", value: 1200, change: 10, change_percent: 0.8 },
        vn30: { name: "VN30", value: 1230, change: 9, change_percent: 0.7 },
        hnx: { name: "HNX-Index", value: 230, change: -1, change_percent: -0.4 },
        upcom: { name: "UPCOM", value: 90, change: 0.2, change_percent: 0.2 },
      })
      .mockResolvedValueOnce([
        { time: "2026-04-01", close: 1200, symbol: "VNINDEX" },
        { time: "2026-04-02", close: 1210, symbol: "VNINDEX" },
      ])
      .mockResolvedValueOnce({
        gainers: movers,
        losers: movers,
        most_active: movers,
      });
    apiAuthGetMock.mockResolvedValueOnce([]);

    const data = await getDashboardData();

    expect(data.topGainers.length).toBe(10);
    expect(data.topLosers.length).toBe(10);
    expect(data.topMostActive.length).toBe(10);
  });

  it("fallback dữ liệu khi top movers trả rỗng", async () => {
    apiGetMock
      .mockResolvedValueOnce({
        vnindex: { name: "VN-Index", value: 1200, change: 10, change_percent: 0.8 },
        vn30: { name: "VN30", value: 1230, change: 9, change_percent: 0.7 },
        hnx: { name: "HNX-Index", value: 230, change: -1, change_percent: -0.4 },
        upcom: { name: "UPCOM", value: 90, change: 0.2, change_percent: 0.2 },
      })
      .mockResolvedValueOnce([
        { time: "2026-04-01", close: 1200, symbol: "VNINDEX" },
      ])
      .mockResolvedValueOnce({
        gainers: [],
        losers: [],
        most_active: [],
      });
    apiAuthGetMock.mockResolvedValueOnce([]);

    const data = await getDashboardData();

    expect(data.topGainers.length).toBe(dashboardMock.topGainers.length);
    expect(data.topLosers.length).toBe(dashboardMock.topLosers.length);
    expect(data.topMostActive.length).toBe(dashboardMock.topMostActive.length);
    expect(data.topGainers[0]?.symbol).toBe(dashboardMock.topGainers[0].symbol);
  });
});
