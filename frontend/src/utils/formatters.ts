export function parseCurrencyInput(rawValue: string): number {
  const digitsOnly = rawValue.replace(/[^\d]/g, "");
  if (!digitsOnly) {
    return 0;
  }

  return Number(digitsOnly);
}

export function formatCurrencyInput(value: number): string {
  const safeValue = Number.isFinite(value) && value > 0 ? Math.round(value) : 0;
  return new Intl.NumberFormat("vi-VN").format(safeValue);
}
