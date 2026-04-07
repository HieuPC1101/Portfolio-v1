import { useQuery } from "@tanstack/react-query";
import { getPortfolioList } from "@/repositories/portfolioRepository";

export function usePortfolioQuery() {
  return useQuery({
    queryKey: ["portfolio-list"],
    queryFn: getPortfolioList,
  });
}
