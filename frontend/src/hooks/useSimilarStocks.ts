import { useQuery } from "@tanstack/react-query";
import { getSimilarStocks } from "@/repositories/portfolioSuggestionRepository";

export function useSimilarStocks(symbol: string, limit = 5) {
  const normalizedSymbol = symbol.trim().toUpperCase();

  return useQuery({
    queryKey: ["similar-stocks", normalizedSymbol, limit],
    queryFn: () => getSimilarStocks(normalizedSymbol, limit),
    enabled: normalizedSymbol.length > 0,
    staleTime: 3 * 60 * 1000,
  });
}
