import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: false,
  MOCK_API_DELAY_MS: 0,
}));

const apiGetMock = vi.fn();
const apiAuthGetMock = vi.fn();

vi.mock("@/lib/api", () => ({
  apiGet: (path: string) => apiGetMock(path),
}));

vi.mock("@/lib/apiAuth", () => ({
  apiAuthGet: (path: string) => apiAuthGetMock(path),
  apiAuthPost: vi.fn(),
  apiAuthDelete: vi.fn(),
}));

import { getWatchlist } from "@/repositories/watchlistRepository";

describe("getWatchlist live mode", () => {
  beforeEach(() => {
    apiGetMock.mockReset();
    apiAuthGetMock.mockReset();
  });

  it("fallback sang API price khi ma khong nam trong top movers", async () => {
    apiAuthGetMock.mockResolvedValue([
      {
        id: 1,
        name: "Danh sach theo doi",
        stocks: [{ id: 101, symbol: "FPT" }],
      },
    ]);

    apiGetMock.mockImplementation((path: string) => {
      if (path === "/api/v1/market/top-movers?top_n=50") {
        return Promise.resolve({ gainers: [], losers: [] });
      }

      if (path.startsWith("/api/v1/market/stock/FPT/price?")) {
        return Promise.resolve({
          data: {
            prices: [
              { date: "2026-04-08", close: 100 },
              { date: "2026-04-09", close: 105 },
            ],
          },
        });
      }

      throw new Error(`Unexpected path ${path}`);
    });

    const data = await getWatchlist();

    expect(data.items).toHaveLength(1);
    expect(data.items[0].symbol).toBe("FPT");
    expect(data.items[0].price).toBe(105);
    expect(data.items[0].percent).toBeCloseTo(5, 6);
    expect(data.items[0].change).toBe(5);
    expect(apiGetMock).toHaveBeenCalledWith("/api/v1/market/top-movers?top_n=50");
    expect(apiGetMock).toHaveBeenCalledWith(expect.stringMatching(/^\/api\/v1\/market\/stock\/FPT\/price\?/));
  });
});
