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
import { useDeleteStock } from "@/hooks/useDeleteStock";
import type { PortfolioHolding } from "@/types/portfolio";

interface DeleteStockDialogProps {
  portfolioId: string | null;
  stock: PortfolioHolding | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function DeleteStockDialog({
  portfolioId,
  stock,
  open,
  onOpenChange,
}: DeleteStockDialogProps) {
  const deleteStockMutation = useDeleteStock();

  async function handleDelete() {
    if (!portfolioId || !stock) {
      return;
    }

    await deleteStockMutation.mutateAsync({
      portfolioId,
      stockId: stock.id,
    });

    onOpenChange(false);
  }

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Xóa cổ phiếu</AlertDialogTitle>
          <AlertDialogDescription>
            Bạn có chắc muốn xóa cổ phiếu <strong>{stock?.symbol ?? ""}</strong> khỏi danh mục?
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={deleteStockMutation.isPending}>Hủy</AlertDialogCancel>
          <AlertDialogAction
            onClick={(event) => {
              event.preventDefault();
              void handleDelete();
            }}
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            disabled={deleteStockMutation.isPending}
          >
            {deleteStockMutation.isPending ? "Đang xóa..." : "Xóa"}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
