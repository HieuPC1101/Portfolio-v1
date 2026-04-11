import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

import { addWatchlistSymbol, getWatchlist, removeWatchlistSymbol } from "@/repositories/watchlistRepository";

describe("getWatchlist", () => {
  it("trả về watchlist data với items", async () => {
    const data = await getWatchlist();

    expect(data.items.length).toBeGreaterThan(0);
    expect(data.items[0]).toHaveProperty("symbol");
    expect(data.items[0]).toHaveProperty("percent");
  });

  it("thêm và xóa mã trong watchlist mock", async () => {
    const symbol = "BIDX";

    await addWatchlistSymbol({ symbol });
    const afterAdd = await getWatchlist();
    expect(afterAdd.items.some((item) => item.symbol === symbol)).toBe(true);

    await removeWatchlistSymbol({ symbol });
    const afterRemove = await getWatchlist();
    expect(afterRemove.items.some((item) => item.symbol === symbol)).toBe(false);
  });
});
