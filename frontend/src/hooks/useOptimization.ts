import { useMutation, useQuery } from "@tanstack/react-query";
import {
  calculateOptimization,
  getOptimizationAlgorithms,
} from "@/repositories/optimizeRepository";
import type { CalculateOptimizationPayload } from "@/types/optimize";

export function useOptimizationAlgorithms() {
  return useQuery({
    queryKey: ["optimization-algorithms"],
    queryFn: getOptimizationAlgorithms,
  });
}

export function useOptimization() {
  return useMutation({
    mutationFn: (payload: CalculateOptimizationPayload) => calculateOptimization(payload),
  });
}
