import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { updatePortfolioStock } from "@/repositories/portfolioRepository";
import type { UpdateStockPayload } from "@/types/portfolio";

interface UpdateStockArgs {
  portfolioId: string;
  stockId: string;
  payload: UpdateStockPayload;
}

export function useUpdateStock() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ portfolioId, stockId, payload }: UpdateStockArgs) => updatePortfolioStock(portfolioId, stockId, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["portfolio-list"] });
      queryClient.invalidateQueries({ queryKey: ["dashboard"] });
      toast.success("Cập nhật cổ phiếu thành công");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại");
    },
  });
}
