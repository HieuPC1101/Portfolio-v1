import { cn } from "@/lib/utils";
import { formatNumber } from "@/lib/format";

interface PriceTagProps {
  value: number;
  change?: number;
  changePercent?: number;
  className?: string;
}

export function PriceTag({ value, change, changePercent, className }: PriceTagProps) {
  const isPositive = (change ?? 0) > 0;
  const isNegative = (change ?? 0) < 0;

  return (
    <span className={cn("inline-flex items-center gap-2 tabular-nums", className)}>
      <span className="font-semibold">{formatNumber(value)}</span>
      {change !== undefined && (
        <span className={cn(
          "text-sm",
          isPositive && "text-stock-up",
          isNegative && "text-stock-down",
          !isPositive && !isNegative && "text-muted-foreground"
        )}>
          {isPositive ? '+' : ''}{formatNumber(change)}
          {changePercent !== undefined && (
            <span className="ml-1">({isPositive ? '+' : ''}{changePercent.toFixed(2)}%)</span>
          )}
        </span>
      )}
    </span>
  );
}
