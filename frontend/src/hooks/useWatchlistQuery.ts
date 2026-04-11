import { useQuery } from "@tanstack/react-query";
import { getWatchlist } from "@/repositories/watchlistRepository";

export function useWatchlistQuery() {
  return useQuery({
    queryKey: ["watchlist"],
    queryFn: getWatchlist,
  });
}
