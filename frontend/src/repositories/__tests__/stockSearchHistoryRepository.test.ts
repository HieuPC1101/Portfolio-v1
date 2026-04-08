import { beforeEach, describe, expect, it } from "vitest";
import {
  clearStockSearchHistory,
  getStockSearchHistory,
  removeStockSearchHistory,
  saveStockSearchHistory,
} from "@/repositories/stockSearchHistoryRepository";

const TEST_STORAGE_KEY = "test:stock-history";

describe("stockSearchHistoryRepository", () => {
  beforeEach(() => {
    clearStockSearchHistory(TEST_STORAGE_KEY);
  });

  it("lưu lịch sử tìm kiếm theo thứ tự mới nhất", () => {
    saveStockSearchHistory(
      {
        symbol: "fpt",
        name: "FPT Corporation",
        exchange: "HOSE",
        sector: "Công nghệ",
        price: 132500,
        percent: 2.71,
      },
      TEST_STORAGE_KEY,
    );

    saveStockSearchHistory(
      {
        symbol: "VCB",
        name: "Vietcombank",
        exchange: "HOSE",
        sector: "Ngân hàng",
        price: 85400,
        percent: 1.43,
      },
      TEST_STORAGE_KEY,
    );

    const history = getStockSearchHistory(TEST_STORAGE_KEY);
    expect(history[0].symbol).toBe("VCB");
    expect(history[1].symbol).toBe("FPT");
  });

  it("ghi đè bản ghi cũ nếu tìm lại cùng mã", () => {
    saveStockSearchHistory(
      {
        symbol: "FPT",
        name: "FPT Corporation",
        exchange: "HOSE",
        sector: "Công nghệ",
        price: 132500,
        percent: 2.71,
      },
      TEST_STORAGE_KEY,
    );

    saveStockSearchHistory(
      {
        symbol: "fpt",
        name: "FPT Corp",
        exchange: "HOSE",
        sector: "Công nghệ",
        price: 132000,
        percent: 2.6,
      },
      TEST_STORAGE_KEY,
    );

    const history = getStockSearchHistory(TEST_STORAGE_KEY);
    expect(history.length).toBe(1);
    expect(history[0].symbol).toBe("FPT");
    expect(history[0].name).toBe("FPT Corp");
  });

  it("xóa từng mã và xóa toàn bộ lịch sử", () => {
    saveStockSearchHistory(
      {
        symbol: "FPT",
        name: "FPT Corporation",
        exchange: "HOSE",
        sector: "Công nghệ",
        price: 132500,
        percent: 2.71,
      },
      TEST_STORAGE_KEY,
    );
    saveStockSearchHistory(
      {
        symbol: "VCB",
        name: "Vietcombank",
        exchange: "HOSE",
        sector: "Ngân hàng",
        price: 85400,
        percent: 1.43,
      },
      TEST_STORAGE_KEY,
    );

    const afterRemove = removeStockSearchHistory("FPT", TEST_STORAGE_KEY);
    expect(afterRemove.length).toBe(1);
    expect(afterRemove[0].symbol).toBe("VCB");

    clearStockSearchHistory(TEST_STORAGE_KEY);
    expect(getStockSearchHistory(TEST_STORAGE_KEY)).toEqual([]);
  });
});
