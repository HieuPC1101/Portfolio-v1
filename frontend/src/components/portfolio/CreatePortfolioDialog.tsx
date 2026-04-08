import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useCreatePortfolio } from "@/hooks/useCreatePortfolio";
import { formatCurrencyInput, parseCurrencyInput } from "@/utils/formatters";

const formSchema = z.object({
  name: z.string().trim().min(1, "Tên danh mục là bắt buộc").max(100, "Tối đa 100 ký tự"),
  description: z.string().max(500, "Tối đa 500 ký tự").optional(),
  totalInvestment: z.coerce.number().gt(0, "Tổng vốn đầu tư phải lớn hơn 0"),
});

type FormValues = z.infer<typeof formSchema>;

interface CreatePortfolioDialogProps {
  triggerLabel?: string;
  triggerClassName?: string;
  onCreated?: (portfolioId: string) => void;
}

export function CreatePortfolioDialog({
  triggerLabel = "Tạo danh mục",
  triggerClassName,
  onCreated,
}: CreatePortfolioDialogProps) {
  const [open, setOpen] = useState(false);
  const [totalInvestmentInput, setTotalInvestmentInput] = useState(() => formatCurrencyInput(100000000));
  const createPortfolioMutation = useCreatePortfolio();

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
      totalInvestment: 100000000,
    },
  });

  async function onSubmit(values: FormValues) {
    const created = await createPortfolioMutation.mutateAsync({
      name: values.name,
      description: values.description?.trim() || undefined,
      totalInvestment: values.totalInvestment,
    });

    onCreated?.(created.id);
    reset();
    setTotalInvestmentInput(formatCurrencyInput(100000000));
    setOpen(false);
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button className={triggerClassName}>
          <Plus className="h-4 w-4" /> {triggerLabel}
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Tạo danh mục mới</DialogTitle>
          <DialogDescription>
            Tạo danh mục đầu tư để theo dõi hiệu suất cổ phiếu của bạn.
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <input type="hidden" {...register("totalInvestment")} />

          <div className="space-y-1.5">
            <Label htmlFor="portfolio-name">Tên danh mục</Label>
            <Input
              id="portfolio-name"
              placeholder="Ví dụ: Danh mục dài hạn"
              {...register("name")}
            />
            {errors.name && <p className="text-xs text-destructive">{errors.name.message}</p>}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="portfolio-description">Mô tả (tùy chọn)</Label>
            <Textarea
              id="portfolio-description"
              rows={3}
              placeholder="Mô tả chiến lược hoặc mục tiêu danh mục"
              {...register("description")}
            />
            {errors.description && (
              <p className="text-xs text-destructive">{errors.description.message}</p>
            )}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="portfolio-total">Tổng vốn đầu tư (VND)</Label>
            <Input
              id="portfolio-total"
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
              onClick={() => setOpen(false)}
              disabled={createPortfolioMutation.isPending}
            >
              Hủy
            </Button>
            <Button type="submit" disabled={createPortfolioMutation.isPending}>
              {createPortfolioMutation.isPending ? "Đang tạo..." : "Tạo danh mục"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
