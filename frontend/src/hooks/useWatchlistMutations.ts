import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { addWatchlistSymbol, removeWatchlistSymbol } from "@/repositories/watchlistRepository";

export function useAddWatchlistSymbol() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: addWatchlistSymbol,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["watchlist"] });
      queryClient.invalidateQueries({ queryKey: ["notifications"] });
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Co loi xay ra, vui long thu lai");
    },
  });
}

export function useRemoveWatchlistSymbol() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: removeWatchlistSymbol,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["watchlist"] });
      queryClient.invalidateQueries({ queryKey: ["notifications"] });
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Co loi xay ra, vui long thu lai");
    },
  });
}
