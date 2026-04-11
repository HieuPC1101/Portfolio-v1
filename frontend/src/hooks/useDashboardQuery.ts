import { useQuery } from "@tanstack/react-query";
import { getDashboardData } from "@/repositories/dashboardRepository";

export const DASHBOARD_REFETCH_INTERVAL_MS = 3_600_000;
export const DASHBOARD_REFETCH_INTERVAL_MINUTES = DASHBOARD_REFETCH_INTERVAL_MS / 60_000;

export function useDashboardQuery() {
  return useQuery({
    queryKey: ["dashboard"],
    queryFn: getDashboardData,
    refetchInterval: DASHBOARD_REFETCH_INTERVAL_MS,
    staleTime: 3_540_000,
    refetchIntervalInBackground: false,
  });
}
