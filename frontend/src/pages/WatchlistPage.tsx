import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { useWatchlistQuery } from "@/hooks/useWatchlistQuery";
import { Plus, X } from "lucide-react";

export default function WatchlistPage() {
  const { data, isPending, isError } = useWatchlistQuery();

  if (isPending) {
    return (
      <div className="flex items-center justify-center h-64">
        <LoadingSpinner />
      </div>
    );
  }

  if (isError || !data) {
    return <div className="text-sm text-muted-foreground">Không thể tải dữ liệu.</div>;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Watchlist</h1>
          <p className="text-muted-foreground text-sm mt-1">Theo dõi các mã cổ phiếu quan tâm</p>
        </div>
        <Button className="gap-2">
          <Plus className="h-4 w-4" /> Thêm mã
        </Button>
      </div>

      {data.items.length === 0 ? (
        <div className="text-sm text-muted-foreground">Watchlist hiện đang trống.</div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
          {data.items.map((stock) => (
            <Card key={stock.symbol} className="bg-card border-border hover:border-primary/50 transition-colors cursor-pointer group">
              <CardContent className="p-4">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="font-bold text-lg">{stock.symbol}</p>
                    <p className="text-xs text-muted-foreground">{stock.name}</p>
                  </div>
                  <Button variant="ghost" size="icon" className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity text-muted-foreground hover:text-destructive">
                    <X className="h-3 w-3" />
                  </Button>
                </div>
                <div className="mt-3 flex items-end justify-between">
                  <p className="text-xl font-bold tabular-nums">{stock.price.toLocaleString("vi-VN")}</p>
                  <PercentBadge value={stock.percent} />
                </div>
                <p className={`text-xs mt-1 tabular-nums ${stock.change > 0 ? "text-stock-up" : "text-stock-down"}`}>
                  {stock.change > 0 ? "+" : ""}
                  {stock.change.toLocaleString("vi-VN")}
                </p>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
