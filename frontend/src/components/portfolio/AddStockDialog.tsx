import { useEffect, useMemo, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useQuery } from "@tanstack/react-query";
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
import { searchStocks } from "@/repositories/marketRepository";
import { useAddStock } from "@/hooks/useAddStock";

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

  const debouncedQuery = useMemo(() => searchValue.trim(), [searchValue]);

  const { data: stockOptions, isFetching } = useQuery({
    queryKey: ["stock-search-dialog", debouncedQuery],
    queryFn: () => searchStocks(debouncedQuery, 6),
    enabled: debouncedQuery.length >= 1,
  });

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
            <Input
              id="add-stock-symbol"
              placeholder="Ví dụ: FPT"
              value={searchValue}
              onChange={(event) => {
                const nextValue = event.target.value.toUpperCase();
                setSearchValue(nextValue);
                setValue("symbol", nextValue, { shouldValidate: true });
              }}
            />
            {isFetching && <p className="text-xs text-muted-foreground">Đang tìm kiếm mã cổ phiếu...</p>}
            {stockOptions && stockOptions.length > 0 && searchValue.trim().length > 0 && (
              <div className="rounded-md border border-border max-h-40 overflow-y-auto">
                {stockOptions.map((option) => (
                  <button
                    key={`${option.symbol}-${option.exchange ?? "na"}`}
                    type="button"
                    className="w-full px-3 py-2 text-left text-sm hover:bg-accent"
                    onClick={() => {
                      setSearchValue(option.symbol);
                      setValue("symbol", option.symbol, { shouldValidate: true });
                    }}
                  >
                    [{option.symbol}] {option.name ?? "Không có tên"}
                  </button>
                ))}
              </div>
            )}
            {errors.symbol && <p className="text-xs text-destructive">{errors.symbol.message}</p>}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="add-stock-shares">Số lượng</Label>
            <Input id="add-stock-shares" type="number" min={1} step={1} {...register("shares")} />
            {errors.shares && <p className="text-xs text-destructive">{errors.shares.message}</p>}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="add-stock-price">Giá mua (VND)</Label>
            <Input id="add-stock-price" type="number" min={1} step={100} {...register("purchasePrice")} />
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
