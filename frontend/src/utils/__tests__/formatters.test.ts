import { describe, expect, it } from "vitest";
import { formatCurrencyInput, parseCurrencyInput } from "@/utils/formatters";

describe("currency formatters", () => {
  it("parseCurrencyInput chuyển chuỗi thành số", () => {
    expect(parseCurrencyInput("100,500,000")).toBe(100500000);
    expect(parseCurrencyInput("abc")).toBe(0);
  });

  it("formatCurrencyInput format theo locale vi-VN", () => {
    expect(formatCurrencyInput(100500000)).toBe("100.500.000");
    expect(formatCurrencyInput(0)).toBe("0");
  });
});
