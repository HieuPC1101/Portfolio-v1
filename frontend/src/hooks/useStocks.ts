import { useQuery } from "@tanstack/react-query";
import { searchOptimizationStocks } from "@/repositories/optimizeRepository";

export function useStocks(query: string, limit = 8) {
  const normalizedQuery = query.trim();

  return useQuery({
    queryKey: ["optimization-stock-search", normalizedQuery, limit],
    queryFn: () => searchOptimizationStocks(normalizedQuery, limit),
    enabled: normalizedQuery.length >= 1,
  });
}
