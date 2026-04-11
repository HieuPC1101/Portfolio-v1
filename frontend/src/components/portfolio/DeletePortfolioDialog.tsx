import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { useDeletePortfolio } from "@/hooks/useDeletePortfolio";
import type { PortfolioItem } from "@/types/portfolio";

interface DeletePortfolioDialogProps {
  portfolio: PortfolioItem | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function DeletePortfolioDialog({
  portfolio,
  open,
  onOpenChange,
}: DeletePortfolioDialogProps) {
  const deletePortfolioMutation = useDeletePortfolio();

  async function handleDelete() {
    if (!portfolio) {
      return;
    }

    await deletePortfolioMutation.mutateAsync(portfolio.id);
    onOpenChange(false);
  }

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Xóa danh mục</AlertDialogTitle>
          <AlertDialogDescription>
            Bạn có chắc muốn xóa danh mục <strong>{portfolio?.name ?? ""}</strong>? Tất cả cổ phiếu
            trong danh mục này cũng sẽ bị xóa.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={deletePortfolioMutation.isPending}>Hủy</AlertDialogCancel>
          <AlertDialogAction
            onClick={(event) => {
              event.preventDefault();
              void handleDelete();
            }}
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            disabled={deletePortfolioMutation.isPending}
          >
            {deletePortfolioMutation.isPending ? "Đang xóa..." : "Xóa"}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
