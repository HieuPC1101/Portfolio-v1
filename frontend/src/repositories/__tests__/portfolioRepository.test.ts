import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

import { getPortfolioList } from "@/repositories/portfolioRepository";

describe("getPortfolioList", () => {
  it("trả về danh sách portfolio với holdings", async () => {
    const data = await getPortfolioList();

    expect(data.portfolios.length).toBeGreaterThan(0);
    expect(data.portfolios[0].holdings.length).toBeGreaterThan(0);
    expect(data.portfolios[0]).toHaveProperty("pnlPercent");
  });
});
