import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

import { getWatchlist } from "@/repositories/watchlistRepository";

describe("getWatchlist", () => {
  it("trả về watchlist data với items", async () => {
    const data = await getWatchlist();

    expect(data.items.length).toBeGreaterThan(0);
    expect(data.items[0]).toHaveProperty("symbol");
    expect(data.items[0]).toHaveProperty("percent");
  });
});
