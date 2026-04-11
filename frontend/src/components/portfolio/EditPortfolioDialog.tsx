import { useEffect, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { Button } from "@/components/ui/button";
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
import { Textarea } from "@/components/ui/textarea";
import { useUpdatePortfolio } from "@/hooks/useUpdatePortfolio";
import type { PortfolioItem } from "@/types/portfolio";
import { formatCurrencyInput, parseCurrencyInput } from "@/utils/formatters";

const formSchema = z.object({
  name: z.string().trim().min(1, "Tên danh mục là bắt buộc").max(100, "Tối đa 100 ký tự"),
  description: z.string().max(500, "Tối đa 500 ký tự").optional(),
  totalInvestment: z.coerce.number().gt(0, "Tổng vốn đầu tư phải lớn hơn 0"),
});

type FormValues = z.infer<typeof formSchema>;

interface EditPortfolioDialogProps {
  portfolio: PortfolioItem | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function EditPortfolioDialog({ portfolio, open, onOpenChange }: EditPortfolioDialogProps) {
  const updatePortfolioMutation = useUpdatePortfolio();
  const [totalInvestmentInput, setTotalInvestmentInput] = useState("");
  const {
    register,
    handleSubmit,
    setValue,
    formState: { errors },
    reset,
  } = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      name: "",
      description: "",
      totalInvestment: 0,
    },
  });

  useEffect(() => {
    if (!portfolio) {
      return;
    }

    reset({
      name: portfolio.name,
      description: portfolio.description ?? "",
      totalInvestment: portfolio.totalInvested,
    });
    setTotalInvestmentInput(formatCurrencyInput(portfolio.totalInvested));
  }, [portfolio, reset]);

  async function onSubmit(values: FormValues) {
    if (!portfolio) {
      return;
    }

    await updatePortfolioMutation.mutateAsync({
      id: portfolio.id,
      payload: {
        name: values.name,
        description: values.description?.trim() || undefined,
        totalInvestment: values.totalInvestment,
      },
    });

    onOpenChange(false);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Sửa danh mục</DialogTitle>
          <DialogDescription>Cập nhật thông tin danh mục đầu tư.</DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <input type="hidden" {...register("totalInvestment")} />

          <div className="space-y-1.5">
            <Label htmlFor="edit-portfolio-name">Tên danh mục</Label>
            <Input id="edit-portfolio-name" {...register("name")} />
            {errors.name && <p className="text-xs text-destructive">{errors.name.message}</p>}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="edit-portfolio-description">Mô tả (tùy chọn)</Label>
            <Textarea id="edit-portfolio-description" rows={3} {...register("description")} />
            {errors.description && (
              <p className="text-xs text-destructive">{errors.description.message}</p>
            )}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="edit-portfolio-total">Tổng vốn đầu tư (VND)</Label>
            <Input
              id="edit-portfolio-total"
              type="text"
              inputMode="numeric"
              value={totalInvestmentInput}
              onChange={(event) => {
                const rawValue = event.target.value;
                const parsedValue = parseCurrencyInput(rawValue);

                if (!rawValue.trim()) {
                  setTotalInvestmentInput("");
                  setValue("totalInvestment", 0, { shouldDirty: true, shouldValidate: true });
                  return;
                }

                setTotalInvestmentInput(formatCurrencyInput(parsedValue));
                setValue("totalInvestment", parsedValue, { shouldDirty: true, shouldValidate: true });
              }}
              onBlur={() => {
                const parsedValue = parseCurrencyInput(totalInvestmentInput);
                setTotalInvestmentInput(parsedValue > 0 ? formatCurrencyInput(parsedValue) : "");
              }}
            />
            {errors.totalInvestment && (
              <p className="text-xs text-destructive">{errors.totalInvestment.message}</p>
            )}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={updatePortfolioMutation.isPending}
            >
              Hủy
            </Button>
            <Button type="submit" disabled={updatePortfolioMutation.isPending}>
              {updatePortfolioMutation.isPending ? "Đang lưu..." : "Lưu thay đổi"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
