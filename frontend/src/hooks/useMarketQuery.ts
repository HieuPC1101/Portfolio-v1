import { useQuery } from "@tanstack/react-query";
import { getMarketData } from "@/repositories/marketRepository";

export function useMarketQuery() {
  return useQuery({
    queryKey: ["market"],
    queryFn: getMarketData,
  });
}
