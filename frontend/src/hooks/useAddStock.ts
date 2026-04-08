import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { addStockToPortfolio } from "@/repositories/portfolioRepository";
import type { AddStockPayload } from "@/types/portfolio";

interface AddStockArgs {
  portfolioId: string;
  payload: AddStockPayload;
}

export function useAddStock() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ portfolioId, payload }: AddStockArgs) => addStockToPortfolio(portfolioId, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["portfolio-list"] });
      toast.success("Thêm cổ phiếu thành công");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại");
    },
  });
}
