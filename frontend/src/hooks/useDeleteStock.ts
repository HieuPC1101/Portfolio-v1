import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { deletePortfolioStock } from "@/repositories/portfolioRepository";

interface DeleteStockArgs {
  portfolioId: string;
  stockId: string;
}

export function useDeleteStock() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ portfolioId, stockId }: DeleteStockArgs) => deletePortfolioStock(portfolioId, stockId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["portfolio-list"] });
      queryClient.invalidateQueries({ queryKey: ["dashboard"] });
      toast.success("Xóa cổ phiếu thành công");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại");
    },
  });
}
