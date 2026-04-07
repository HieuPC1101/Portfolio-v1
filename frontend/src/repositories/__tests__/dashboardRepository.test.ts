import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

import { getDashboardData } from "@/repositories/dashboardRepository";

describe("getDashboardData", () => {
  it("trả về dữ liệu dashboard mock", async () => {
    const data = await getDashboardData();

    expect(data.indices.length).toBeGreaterThan(0);
    expect(data.topMovers.length).toBeGreaterThan(0);
    expect(data.summary.portfolioCount).toBe(3);
    expect(data.summary.stockCount).toBe(12);
  });
});
