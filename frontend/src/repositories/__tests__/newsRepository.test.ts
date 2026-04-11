import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

import { getNewsFeed } from "@/repositories/newsRepository";

describe("getNewsFeed", () => {
  it("trả về feed tin tức có item đại diện", async () => {
    const data = await getNewsFeed();

    expect(data.items.length).toBeGreaterThan(0);
    expect(data.items[0]).toHaveProperty("title");
    expect(data.items[0]).toHaveProperty("source");
  });
});
