import { useQuery } from "@tanstack/react-query";
import { getPortfolioSuggestionData } from "@/repositories/portfolioSuggestionRepository";

export function usePortfolioSuggestions() {
  return useQuery({
    queryKey: ["portfolio-suggestions"],
    queryFn: getPortfolioSuggestionData,
    staleTime: 5 * 60 * 1000,
  });
}
