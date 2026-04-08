import { useEffect, useState } from "react";
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
import { useAddStock } from "@/hooks/useAddStock";
import { StockAutocomplete } from "@/components/portfolio/StockAutocomplete";

const formSchema = z.object({
  symbol: z.string().trim().min(1, "Mã cổ phiếu là bắt buộc").max(10).transform((value) => value.toUpperCase()),
  shares: z.coerce.number().int("Số lượng phải là số nguyên").gt(0, "Số lượng phải lớn hơn 0"),
  purchasePrice: z.coerce.number().gt(0, "Giá mua phải lớn hơn 0"),
});

type FormValues = z.infer<typeof formSchema>;

interface AddStockDialogProps {
  portfolioId: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function AddStockDialog({ portfolioId, open, onOpenChange }: AddStockDialogProps) {
  const addStockMutation = useAddStock();
  const [searchValue, setSearchValue] = useState("");

  const {
    register,
    handleSubmit,
    setValue,
    watch,
    formState: { errors },
    reset,
  } = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      symbol: "",
      shares: 100,
      purchasePrice: 10000,
    },
  });

  const symbolValue = watch("symbol");

  useEffect(() => {
    setSearchValue(symbolValue ?? "");
  }, [symbolValue]);

  useEffect(() => {
    if (!open) {
      setSearchValue("");
      reset({
        symbol: "",
        shares: 100,
        purchasePrice: 10000,
      });
    }
  }, [open, reset]);

  async function onSubmit(values: FormValues) {
    if (!portfolioId) {
      return;
    }

    await addStockMutation.mutateAsync({
      portfolioId,
      payload: {
        symbol: values.symbol,
        shares: values.shares,
        purchasePrice: values.purchasePrice,
      },
    });

    reset();
    onOpenChange(false);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Thêm cổ phiếu</DialogTitle>
          <DialogDescription>Thêm cổ phiếu mới vào danh mục hiện tại.</DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <input type="hidden" {...register("symbol")} />

          <div className="space-y-1.5">
            <Label htmlFor="add-stock-symbol">Mã cổ phiếu</Label>
            <StockAutocomplete
              inputId="add-stock-symbol"
              value={searchValue}
              onValueChange={(nextValue) => {
                setSearchValue(nextValue);
                setValue("symbol", nextValue, { shouldValidate: true });
              }}
              onSelect={(stock) => {
                setSearchValue(stock.symbol);
                setValue("symbol", stock.symbol, { shouldValidate: true });

                if (typeof stock.price === "number" && stock.price > 0) {
                  setValue("purchasePrice", stock.price, { shouldValidate: true });
                }
              }}
              placeholder="Ví dụ: FPT hoặc Vinamilk"
            />
            {errors.symbol && <p className="text-xs text-destructive">{errors.symbol.message}</p>}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="add-stock-shares">Số lượng</Label>
            <Input id="add-stock-shares" type="number" min={1} step={1} {...register("shares")} />
            {errors.shares && <p className="text-xs text-destructive">{errors.shares.message}</p>}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="add-stock-price">Giá mua (VND)</Label>
            <Input id="add-stock-price" type="number" min={0} step={100} {...register("purchasePrice")} />
            {errors.purchasePrice && (
              <p className="text-xs text-destructive">{errors.purchasePrice.message}</p>
            )}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={addStockMutation.isPending}
            >
              Hủy
            </Button>
            <Button type="submit" disabled={addStockMutation.isPending || !portfolioId}>
              {addStockMutation.isPending ? "Đang thêm..." : "Thêm cổ phiếu"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
