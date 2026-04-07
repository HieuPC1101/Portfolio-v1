import { useQuery } from "@tanstack/react-query";
import { getDashboardData } from "@/repositories/dashboardRepository";

export function useDashboardQuery() {
  return useQuery({
    queryKey: ["dashboard"],
    queryFn: getDashboardData,
    refetchInterval: 60_000,
    staleTime: 50_000,
    refetchIntervalInBackground: false,
  });
}
