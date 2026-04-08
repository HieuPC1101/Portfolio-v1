import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

import {
  addStockToPortfolio,
  createPortfolio,
  deletePortfolio,
  deletePortfolioStock,
  getPortfolioList,
  updatePortfolio,
} from "@/repositories/portfolioRepository";

describe("getPortfolioList", () => {
  it("trả về danh sách portfolio với holdings", async () => {
    const data = await getPortfolioList();

    expect(data.portfolios.length).toBeGreaterThan(0);
    expect(data.portfolios[0].holdings.length).toBeGreaterThan(0);
    expect(data.portfolios[0]).toHaveProperty("pnlPercent");
  });

  it("tạo mới danh mục thành công", async () => {
    const created = await createPortfolio({
      name: "Danh mục test",
      description: "Mô tả test",
      totalInvestment: 50000000,
    });

    const data = await getPortfolioList();
    expect(data.portfolios.some((portfolio) => portfolio.id === created.id)).toBe(true);
  });

  it("cập nhật và xóa danh mục thành công", async () => {
    const created = await createPortfolio({
      name: "Danh mục sửa/xóa",
      totalInvestment: 20000000,
    });

    const updated = await updatePortfolio(created.id, {
      name: "Danh mục đã sửa",
      totalInvestment: 30000000,
    });

    expect(updated.name).toBe("Danh mục đã sửa");
    expect(updated.totalInvested).toBeGreaterThanOrEqual(0);

    await deletePortfolio(created.id);
    const data = await getPortfolioList();
    expect(data.portfolios.some((portfolio) => portfolio.id === created.id)).toBe(false);
  });

  it("thêm và xóa cổ phiếu trong danh mục", async () => {
    const source = (await getPortfolioList()).portfolios[0];

    const added = await addStockToPortfolio(source.id, {
      symbol: "SSI",
      shares: 100,
      purchasePrice: 25000,
    });

    expect(added.symbol).toBe("SSI");

    const afterAdd = (await getPortfolioList()).portfolios.find((portfolio) => portfolio.id === source.id);
    expect(afterAdd?.holdings.some((holding) => holding.id === added.id)).toBe(true);

    await deletePortfolioStock(source.id, added.id);
    const afterDelete = (await getPortfolioList()).portfolios.find((portfolio) => portfolio.id === source.id);
    expect(afterDelete?.holdings.some((holding) => holding.id === added.id)).toBe(false);
  });
});
