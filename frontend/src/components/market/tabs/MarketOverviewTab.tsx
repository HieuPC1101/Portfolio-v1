import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { MarketData } from "@/types/market";
import { MarketFlowTab } from "@/components/market/tabs/MarketFlowTab";
import { MarketMoversTab } from "@/components/market/tabs/MarketMoversTab";
import { MarketSectorsTab } from "@/components/market/tabs/MarketSectorsTab";
import { ArrowDownLeft, ArrowUpRight, CircleDot } from "lucide-react";

function formatFlowValue(value: number, options?: { withSign?: boolean; absolute?: boolean }) {
  const absolute = options?.absolute ?? false;
  const withSign = options?.withSign ?? false;
  const source = absolute ? Math.abs(value) : value;
  const sign = withSign && source > 0 ? "+" : "";

  return `${sign}${source.toLocaleString("vi-VN")}`;
}

interface MarketOverviewTabProps {
  data: MarketData;
}

export function MarketOverviewTab({ data }: MarketOverviewTabProps) {
  const hasSnapshot = data.foreignFlow.length > 0;
  const latestSnapshot = hasSnapshot ? data.foreignFlow[data.foreignFlow.length - 1] : null;
  const previousSnapshot = data.foreignFlow.length > 1 ? data.foreignFlow[data.foreignFlow.length - 2] : null;
  const buyValue = latestSnapshot ? latestSnapshot.buy : 0;
  const sellValue = latestSnapshot ? Math.abs(latestSnapshot.sell) : 0;
  const netValue = buyValue - sellValue;
  const previousNetValue = previousSnapshot ? previousSnapshot.buy - Math.abs(previousSnapshot.sell) : 0;
  const netDelta = netValue - previousNetValue;
  const grossValue = buyValue + sellValue;
  const buyRatio = grossValue > 0 ? (buyValue / grossValue) * 100 : 0;
  const sellRatio = grossValue > 0 ? 100 - buyRatio : 0;
  const netIntensity = grossValue > 0 ? (Math.abs(netValue) / grossValue) * 100 : 0;
  const netIsPositive = netValue >= 0;
  const recentFlow = data.foreignFlow.slice(-5);
  const recentFlowRows = [...recentFlow]
    .reverse()
    .map((point) => {
      const sell = Math.abs(point.sell);
      const net = point.buy - sell;

      return {
        day: point.day,
        buy: point.buy,
        sell,
        net,
      };
    });
  const recentNetTotal = recentFlowRows.reduce((total, row) => total + row.net, 0);
  const recentPositiveCount = recentFlowRows.filter((row) => row.net >= 0).length;
  const recentNegativeCount = recentFlowRows.length - recentPositiveCount;

  return (
    <div className="space-y-4">
      <MarketSectorsTab data={data} />

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <MarketMoversTab data={data} />

        <Card className="border-border bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Snapshot khối ngoại</CardTitle>
          </CardHeader>
          <CardContent className="flex min-h-[360px] flex-col gap-4">
            {latestSnapshot ? (
              <>
                <div
                  className={`rounded-xl border p-4 ${
                    netIsPositive ? "border-stock-up/40 bg-stock-up/10" : "border-stock-down/40 bg-stock-down/10"
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-xs uppercase tracking-wide text-muted-foreground">Dòng tiền ròng</p>
                    <span
                      className={`rounded-md border px-2 py-1 text-xs font-semibold ${
                        netIsPositive
                          ? "border-stock-up/30 bg-stock-up/15 text-stock-up"
                          : "border-stock-down/30 bg-stock-down/15 text-stock-down"
                      }`}
                    >
                      {netIsPositive ? "Mua ròng" : "Bán ròng"}
                    </span>
                  </div>
                  <div className="mt-2 flex items-end justify-between gap-3">
                    <p className={`text-3xl font-bold tabular-nums ${netIsPositive ? "text-stock-up" : "text-stock-down"}`}>
                      {formatFlowValue(netValue, { withSign: true })}
                    </p>
                  </div>

                  {previousSnapshot ? (
                    <p className="mt-1 text-xs text-muted-foreground">
                      So với phiên trước: <span className={netDelta >= 0 ? "text-stock-up" : "text-stock-down"}>{formatFlowValue(netDelta, { withSign: true })}</span>
                    </p>
                  ) : null}
                </div>

                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  <div className="rounded-lg border border-border/70 bg-accent/30 p-3">
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <ArrowUpRight className="h-3.5 w-3.5 text-stock-up" />
                      <span>Mua</span>
                    </div>
                    <p className="mt-2 text-xl font-semibold tabular-nums text-stock-up">
                      {formatFlowValue(buyValue)}
                    </p>
                  </div>
                  <div className="rounded-lg border border-border/70 bg-accent/30 p-3">
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <ArrowDownLeft className="h-3.5 w-3.5 text-stock-down" />
                      <span>Bán</span>
                    </div>
                    <p className="mt-2 text-xl font-semibold tabular-nums text-stock-down">
                      -{formatFlowValue(sellValue)}
                    </p>
                  </div>
                  <div className="rounded-lg border border-border/70 bg-accent/30 p-3">
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <CircleDot className={`h-3.5 w-3.5 ${netIsPositive ? "text-stock-up" : "text-stock-down"}`} />
                      <span>Ròng</span>
                    </div>
                    <p className={`mt-2 text-xl font-semibold tabular-nums ${netIsPositive ? "text-stock-up" : "text-stock-down"}`}>
                      {formatFlowValue(netValue, { withSign: true })}
                    </p>
                  </div>
                </div>

                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  <div className="rounded-lg border border-border/70 bg-accent/20 p-3">
                    <p className="text-xs text-muted-foreground">Cường độ ròng</p>
                    <p className={`mt-2 text-lg font-semibold tabular-nums ${netIsPositive ? "text-stock-up" : "text-stock-down"}`}>
                      {netIntensity.toFixed(1)}%
                    </p>
                  </div>
                  <div className="rounded-lg border border-border/70 bg-accent/20 p-3">
                    <p className="text-xs text-muted-foreground">Lũy kế 5 phiên</p>
                    <p className={`mt-2 text-lg font-semibold tabular-nums ${recentNetTotal >= 0 ? "text-stock-up" : "text-stock-down"}`}>
                      {formatFlowValue(recentNetTotal, { withSign: true })}
                    </p>
                  </div>
                  <div className="rounded-lg border border-border/70 bg-accent/20 p-3">
                    <p className="text-xs text-muted-foreground">Nhịp mua/bán ròng</p>
                    <p className="mt-2 text-lg font-semibold tabular-nums">
                      <span className="text-stock-up">{recentPositiveCount}</span>
                      <span className="mx-1 text-muted-foreground">/</span>
                      <span className="text-stock-down">{recentNegativeCount}</span>
                    </p>
                  </div>
                </div>

                <div className="rounded-lg border border-border/70 bg-accent/20 p-3">
                  <div className="mb-2 flex items-center justify-between text-xs text-muted-foreground">
                    <span>Tỷ trọng mua/bán</span>
                    <span>Tổng: {formatFlowValue(grossValue)}</span>
                  </div>
                  <div className="flex h-2 overflow-hidden rounded-full bg-muted/70">
                    <div
                      className="h-full bg-stock-up"
                      style={{ width: `${Math.max(0, Math.min(100, buyRatio))}%` }}
                    />
                    <div
                      className="h-full bg-stock-down"
                      style={{ width: `${Math.max(0, Math.min(100, sellRatio))}%` }}
                    />
                  </div>
                  <div className="mt-2 flex items-center justify-between text-xs tabular-nums">
                    <span className="text-stock-up">Mua {buyRatio.toFixed(1)}%</span>
                    <span className="text-stock-down">Bán {sellRatio.toFixed(1)}%</span>
                  </div>
                </div>

                <div className="rounded-lg border border-border/70 bg-accent/20 p-3">
                  <div className="mb-2 flex items-center justify-between text-xs text-muted-foreground">
                    <span>Nhịp giao dịch 5 phiên gần nhất</span>
                    <span>
                      {recentFlowRows.length} phiên
                    </span>
                  </div>
                  <div className="space-y-1.5">
                    {recentFlowRows.map((row) => (
                      <div key={`flow-row-${row.day}`} className="grid grid-cols-[auto_1fr_auto] items-center gap-2 rounded border border-border/50 bg-card/40 px-2 py-1.5 text-xs">
                        <span className="font-medium text-foreground">{row.day}</span>
                        <span className="truncate text-muted-foreground">
                          M {formatFlowValue(row.buy)} | B -{formatFlowValue(row.sell)}
                        </span>
                        <span className={`tabular-nums font-semibold ${row.net >= 0 ? "text-stock-up" : "text-stock-down"}`}>
                          {formatFlowValue(row.net, { withSign: true })}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="mt-auto flex items-center justify-between text-xs text-muted-foreground">
                  <span>
                    Phiên gần nhất: <span className="font-medium text-foreground">{latestSnapshot.day}</span>
                  </span>
                  <span className={netIsPositive ? "text-stock-up" : "text-stock-down"}>
                    {netIsPositive ? "Dòng tiền tích cực" : "Dòng tiền thận trọng"}
                  </span>
                </div>
              </>
            ) : (
              <div className="text-sm text-muted-foreground">Chưa có dữ liệu khối ngoại.</div>
            )}
          </CardContent>
        </Card>
      </div>

      <MarketFlowTab data={data} />
    </div>
  );
}
