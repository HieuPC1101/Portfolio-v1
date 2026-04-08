import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { z } from "zod";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { useUpdateStock } from "@/hooks/useUpdateStock";
import type { PortfolioHolding } from "@/types/portfolio";

const formSchema = z.object({
  shares: z.coerce.number().int("Số lượng phải là số nguyên").gt(0, "Số lượng phải lớn hơn 0"),
  purchasePrice: z.coerce.number().gt(0, "Giá mua phải lớn hơn 0"),
});

type FormValues = z.infer<typeof formSchema>;

interface EditStockDialogProps {
  portfolioId: string | null;
  stock: PortfolioHolding | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function EditStockDialog({ portfolioId, stock, open, onOpenChange }: EditStockDialogProps) {
  const updateStockMutation = useUpdateStock();
  const {
    register,
    handleSubmit,
    formState: { errors },
    reset,
  } = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      shares: 0,
      purchasePrice: 0,
    },
  });

  useEffect(() => {
    if (!stock) {
      return;
    }

    reset({
      shares: stock.shares,
      purchasePrice: stock.avgPrice,
    });
  }, [stock, reset]);

  async function onSubmit(values: FormValues) {
    if (!portfolioId || !stock) {
      return;
    }

    await updateStockMutation.mutateAsync({
      portfolioId,
      stockId: stock.id,
      payload: {
        shares: values.shares,
        purchasePrice: values.purchasePrice,
      },
    });

    onOpenChange(false);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Sửa cổ phiếu</DialogTitle>
          <DialogDescription>Cập nhật vị thế cổ phiếu trong danh mục.</DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <div className="space-y-1.5">
            <Label>Mã cổ phiếu</Label>
            <Input value={stock?.symbol ?? ""} readOnly disabled />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="edit-stock-shares">Số lượng</Label>
            <Input id="edit-stock-shares" type="number" min={1} step={1} {...register("shares")} />
            {errors.shares && <p className="text-xs text-destructive">{errors.shares.message}</p>}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="edit-stock-price">Giá mua (VND)</Label>
            <Input id="edit-stock-price" type="number" min={1} step={100} {...register("purchasePrice")} />
            {errors.purchasePrice && (
              <p className="text-xs text-destructive">{errors.purchasePrice.message}</p>
            )}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={updateStockMutation.isPending}
            >
              Hủy
            </Button>
            <Button type="submit" disabled={updateStockMutation.isPending || !portfolioId || !stock}>
              {updateStockMutation.isPending ? "Đang lưu..." : "Lưu thay đổi"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
