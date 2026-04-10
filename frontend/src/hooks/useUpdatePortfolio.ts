import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { updatePortfolio } from "@/repositories/portfolioRepository";
import type { UpdatePortfolioPayload } from "@/types/portfolio";

interface UpdatePortfolioArgs {
  id: string;
  payload: UpdatePortfolioPayload;
}

export function useUpdatePortfolio() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ id, payload }: UpdatePortfolioArgs) => updatePortfolio(id, payload),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["portfolio-list"] });
      queryClient.invalidateQueries({ queryKey: ["dashboard"] });
      toast.success("Cập nhật danh mục thành công");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại");
    },
  });
}
