import { describe, expect, it } from "vitest";
import { validateOptimizationInput } from "@/utils/validators";

describe("validateOptimizationInput", () => {
  it("trả lỗi khi chưa đủ tối thiểu 2 cổ phiếu", () => {
    const result = validateOptimizationInput({
      selectedStocks: ["VCB"],
      algorithm: "max_sharpe",
      budget: 100000000,
    });

    expect(result.stocks).toContain("Tối thiểu 2 cổ phiếu");
  });

  it("trả lỗi khi thiếu thuật toán hoặc ngân sách không hợp lệ", () => {
    const result = validateOptimizationInput({
      selectedStocks: ["VCB", "FPT"],
      algorithm: "",
      budget: 0,
    });

    expect(result.algorithm).toContain("thuật toán");
    expect(result.budget).toContain("Ngân sách");
  });

  it("không trả lỗi khi dữ liệu hợp lệ", () => {
    const result = validateOptimizationInput({
      selectedStocks: ["VCB", "FPT", "VNM"],
      algorithm: "max_sharpe",
      budget: 150000000,
    });

    expect(result).toEqual({});
  });
});
