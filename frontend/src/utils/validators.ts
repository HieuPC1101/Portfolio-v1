interface ValidateOptimizationInputParams {
  selectedStocks: string[];
  algorithm: string;
  budget: number;
  algorithmParameter?: number | null;
}

interface ValidateOptimizationInputResult {
  stocks?: string;
  algorithm?: string;
  budget?: string;
  algorithmParameter?: string;
}

export function validateOptimizationInput(
  params: ValidateOptimizationInputParams,
): ValidateOptimizationInputResult {
  const errors: ValidateOptimizationInputResult = {};

  if (params.selectedStocks.length < 2) {
    errors.stocks = "Tối thiểu 2 cổ phiếu để tối ưu hóa";
  }

  if (!params.algorithm.trim()) {
    errors.algorithm = "Vui lòng chọn thuật toán";
  }

  if (!Number.isFinite(params.budget) || params.budget <= 0) {
    errors.budget = "Ngân sách phải lớn hơn 0";
  }

  if (params.algorithm === "markowitz") {
    if (!Number.isFinite(params.algorithmParameter) || (params.algorithmParameter ?? 0) <= 0) {
      errors.algorithmParameter = "Target return phải lớn hơn 0%";
    }
  }

  if (params.algorithm === "max_sharpe") {
    if (!Number.isFinite(params.algorithmParameter) || (params.algorithmParameter ?? -1) < 0) {
      errors.algorithmParameter = "Risk-free rate không được âm";
    }
  }

  return errors;
}
