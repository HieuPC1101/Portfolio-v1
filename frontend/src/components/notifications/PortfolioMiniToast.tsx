import { CheckCircle2, Info, TriangleAlert, X } from "lucide-react";
import { toast } from "sonner";
import { cn } from "@/lib/utils";

type PortfolioMiniToastTone = "success" | "error" | "info";

interface PortfolioMiniToastPayload {
  tone: PortfolioMiniToastTone;
  title: string;
  description: string;
}

const toneStyles: Record<
  PortfolioMiniToastTone,
  {
    rail: string;
    iconWrap: string;
    icon: string;
    glow: string;
  }
> = {
  success: {
    rail: "bg-stock-up",
    iconWrap: "bg-stock-up/15",
    icon: "text-stock-up",
    glow: "from-stock-up/30 via-stock-up/10 to-transparent",
  },
  error: {
    rail: "bg-destructive",
    iconWrap: "bg-destructive/15",
    icon: "text-destructive",
    glow: "from-destructive/35 via-destructive/10 to-transparent",
  },
  info: {
    rail: "bg-sky-400",
    iconWrap: "bg-sky-400/15",
    icon: "text-sky-300",
    glow: "from-sky-400/35 via-sky-400/10 to-transparent",
  },
};

function getToneIcon(tone: PortfolioMiniToastTone) {
  if (tone === "success") {
    return CheckCircle2;
  }

  if (tone === "error") {
    return TriangleAlert;
  }

  return Info;
}

export function showPortfolioMiniToast({
  tone,
  title,
  description,
}: PortfolioMiniToastPayload) {
  const styles = toneStyles[tone];
  const ToneIcon = getToneIcon(tone);

  toast.custom(
    (toastId) => (
      <div className="relative w-[340px] overflow-hidden rounded-xl border border-border/70 bg-card/95 shadow-[0_24px_48px_-28px_rgba(0,0,0,0.75)] backdrop-blur">
        <div className={cn("absolute inset-y-0 left-0 w-1", styles.rail)} />
        <div className={cn("pointer-events-none absolute inset-x-0 bottom-0 h-10 bg-gradient-to-r", styles.glow)} />

        <div className="flex items-start gap-3 px-3 py-3.5">
          <div className={cn("mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full", styles.iconWrap)}>
            <ToneIcon className={cn("h-4 w-4", styles.icon)} />
          </div>

          <div className="min-w-0 flex-1">
            <p className="truncate text-sm font-semibold text-foreground">{title}</p>
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{description}</p>
          </div>

          <button
            type="button"
            aria-label="Đóng thông báo"
            onClick={() => toast.dismiss(toastId)}
            className="mt-0.5 inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-md text-muted-foreground transition hover:bg-accent hover:text-foreground"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
    ),
    { duration: tone === "error" ? 4600 : 3400 },
  );
}
