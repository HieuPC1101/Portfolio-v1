import { useQuery } from "@tanstack/react-query";
import { getOptimizeData } from "@/repositories/optimizeRepository";

export function useOptimizeData() {
  return useQuery({
    queryKey: ["optimize-data"],
    queryFn: getOptimizeData,
  });
}
