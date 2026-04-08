import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { createPortfolio } from "@/repositories/portfolioRepository";
import type { CreatePortfolioPayload } from "@/types/portfolio";

export function useCreatePortfolio() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: CreatePortfolioPayload) => createPortfolio(payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["portfolio-list"] });
      toast.success("Tạo danh mục thành công");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại");
    },
  });
}
