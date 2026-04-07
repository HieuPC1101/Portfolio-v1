import { cn } from "@/lib/utils";
import { TrendingUp, TrendingDown } from "lucide-react";

interface PercentBadgeProps {
  value: number;
  className?: string;
  showIcon?: boolean;
}

export function PercentBadge({ value, className, showIcon = true }: PercentBadgeProps) {
  const isPositive = value > 0;
  const isZero = value === 0;

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-xs font-semibold tabular-nums",
        isZero && "bg-muted text-muted-foreground",
        isPositive && "bg-stock-up/15 text-stock-up",
        !isPositive && !isZero && "bg-stock-down/15 text-stock-down",
        className
      )}
    >
      {showIcon && !isZero && (
        isPositive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />
      )}
      {value > 0 ? '+' : ''}{value.toFixed(2)}%
    </span>
  );
}
