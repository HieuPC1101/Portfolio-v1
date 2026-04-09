import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { formatVND } from "@/lib/format";
import { useDashboardQuery } from "@/hooks/useDashboardQuery";
import { dashboardMock } from "@/mocks/dashboard.mock";
import type { DashboardData, TopMover } from "@/types/dashboard";
import { Activity, BarChart3, DollarSign, RefreshCw, TrendingDown, TrendingUp } from "lucide-react";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import { useEffect, useState } from "react";
import type { ComponentType } from "react";

const MAX_TOP_MOVER_ROWS = 10;

function capTopMovers(stocks: TopMover[]): TopMover[] {
  return stocks.slice(0, MAX_TOP_MOVER_ROWS);
}

function resolveTopMoverGroups(data: DashboardData): {
  topGainers: TopMover[];
  topLosers: TopMover[];
  topMostActive: TopMover[];
} {
  const topGainers = capTopMovers(data.topGainers);
  const topLosers = capTopMovers(data.topLosers);
  const topMostActive = capTopMovers(data.topMostActive);

  if (topGainers.length > 0 || topLosers.length > 0 || topMostActive.length > 0) {
    return {
      topGainers,
      topLosers,
      topMostActive,
    };
  }

  return {
    topGainers: capTopMovers(dashboardMock.topGainers),
    topLosers: capTopMovers(dashboardMock.topLosers),
    topMostActive: capTopMovers(dashboardMock.topMostActive),
  };
}

function TopMoverSection({
  title,
  description,
  stocks,
  icon,
  iconClassName,
  emptyText,
  highlightByPercent,
}: {
  title: string;
  description: string;
  stocks: TopMover[];
  icon: ComponentType<{ className?: string }>;
  iconClassName: string;
  emptyText: string;
  highlightByPercent?: boolean;
}) {
  const Icon = icon;
  const visibleStocks = stocks.slice(0, MAX_TOP_MOVER_ROWS);

  return (
    <div className="rounded-lg border border-border/70 bg-accent/30">
      <div className="flex items-center gap-2 border-b border-border/70 px-3 py-2.5">
        <Icon className={`h-4 w-4 ${iconClassName}`} />
        <div>
          <h3 className="text-sm font-semibold">{title}</h3>
          <p className="text-xs text-muted-foreground">{description}</p>
        </div>
      </div>

      {visibleStocks.length > 0 ? (
        <div className="divide-y divide-border/50">
          {visibleStocks.map((stock) => {
            const isUp = stock.percent >= 0;

            return (
              <div key={stock.symbol} className="flex items-center justify-between px-3 py-2.5 hover:bg-accent/50">
                <div>
                  <p className="text-sm font-semibold">{stock.symbol}</p>
                  <p className="text-xs text-muted-foreground tabular-nums">
                    Thanh khoản: {stock.liquidity ? formatVND(stock.liquidity) : "--"}
                  </p>
                </div>
                <div className="text-right">
                  <p className="text-sm tabular-nums">{stock.price.toLocaleString("vi-VN")}</p>
                  <p
                    className={`text-xs tabular-nums ${
                      highlightByPercent ? (isUp ? "text-stock-up" : "text-stock-down") : "text-foreground"
                    }`}
                  >
                    {isUp ? "+" : ""}
                    {stock.percent.toFixed(2)}%
                  </p>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="px-3 py-4 text-sm text-muted-foreground">{emptyText}</div>
      )}
    </div>
  );
}

export default function DashboardPage() {
  const { data, isPending, isError, refetch, isFetching, dataUpdatedAt } = useDashboardQuery();
  const [currentTime, setCurrentTime] = useState(() => Date.now());

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setCurrentTime(Date.now());
    }, 1000);

    return () => window.clearInterval(intervalId);
  }, []);

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

  const hasIndices = data.indices.length > 0;
  const {
    topGainers: visibleTopGainers,
    topLosers: visibleTopLosers,
    topMostActive: visibleTopMostActive,
  } = resolveTopMoverGroups(data);
  const hasTopMovers = visibleTopGainers.length > 0 || visibleTopLosers.length > 0 || visibleTopMostActive.length > 0;
  const lastUpdatedText =
    dataUpdatedAt > 0
      ? new Intl.DateTimeFormat("vi-VN", {
          day: "2-digit",
          month: "2-digit",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          hour12: false,
        }).format(dataUpdatedAt)
      : "--";
  const liveTimeText = new Intl.DateTimeFormat("vi-VN", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(currentTime);

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">Tổng quan</h1>
          <p className="text-muted-foreground text-sm mt-1">Theo dõi thị trường và danh mục đầu tư</p>
          <p className="text-xs text-muted-foreground mt-2 flex items-center gap-2">
            <span className={`inline-block h-2 w-2 rounded-full ${isFetching ? "bg-amber-400 animate-pulse" : "bg-stock-up"}`} />
            <span>{isFetching ? "Đang cập nhật dữ liệu..." : `Realtime: ${liveTimeText}`}</span>
            <span aria-hidden="true">•</span>
            <span>Cập nhật lần cuối: {lastUpdatedText}</span>
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => refetch()}
          disabled={isFetching}
          className="gap-2"
        >
          <RefreshCw className={`h-4 w-4 ${isFetching ? "animate-spin" : ""}`} />
          Làm mới
        </Button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {hasIndices ? (
          data.indices.map((idx) => (
            <Card key={idx.name} className="bg-card border-border">
              <CardContent className="p-4">
                <div className="flex justify-between items-start">
                  <p className="text-sm text-muted-foreground">{idx.name}</p>
                  <PercentBadge value={idx.changePercent} />
                </div>
                <p className="text-xl font-bold mt-2 tabular-nums">{idx.value.toLocaleString("vi-VN")}</p>
                <p className={`text-xs mt-1 tabular-nums ${idx.change > 0 ? "text-stock-up" : "text-stock-down"}`}>
                  {idx.change > 0 ? "+" : ""}
                  {idx.change.toFixed(2)}
                </p>
              </CardContent>
            </Card>
          ))
        ) : (
          <div className="col-span-full text-sm text-muted-foreground">Chưa có dữ liệu chỉ số.</div>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2 bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">VN-Index (30 ngày)</CardTitle>
          </CardHeader>
          <CardContent>
            {data.chart.length > 0 ? (
              <div className="h-[250px]">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={data.chart} margin={{ top: 8, right: 8, left: 0, bottom: 8 }}>
                    <defs>
                      <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(131, 45%, 40%)" stopOpacity={0.3} />
                        <stop offset="95%" stopColor="hsl(131, 45%, 40%)" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <XAxis
                      dataKey="label"
                      axisLine={{ stroke: "hsl(216, 14%, 22%)" }}
                      tickLine={false}
                      tick={{ fill: "hsl(215, 12%, 65%)", fontSize: 11 }}
                      tickMargin={8}
                      minTickGap={24}
                    />
                    <YAxis hide domain={["auto", "auto"]} />
                    <Tooltip
                      contentStyle={{
                        background: "hsl(215, 25%, 11%)",
                        border: "1px solid hsl(216, 14%, 22%)",
                        borderRadius: "8px",
                        color: "hsl(213, 27%, 92%)",
                        fontSize: "12px",
                      }}
                      labelFormatter={(label: string) => `Ngày ${label}`}
                      formatter={(value: number) => [value.toFixed(2), "VN-Index"]}
                    />
                    <Area
                      type="monotone"
                      dataKey="value"
                      stroke="hsl(131, 45%, 40%)"
                      strokeWidth={2}
                      fill="url(#colorValue)"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">Chưa có dữ liệu biểu đồ.</div>
            )}
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Danh mục của tôi</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <p className="text-sm text-muted-foreground">Tổng giá trị</p>
              <p className="text-2xl font-bold">{formatVND(data.summary.totalValue)}</p>
            </div>
            <div className="flex gap-4">
              <div>
                <p className="text-xs text-muted-foreground">Đã đầu tư</p>
                <p className="text-sm font-medium">{formatVND(data.summary.totalInvested)}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Lãi/Lỗ</p>
                <p className={`text-sm font-medium ${data.summary.pnl >= 0 ? "text-stock-up" : "text-stock-down"}`}>
                  {data.summary.pnl >= 0 ? "+" : ""}
                  {formatVND(data.summary.pnl)}
                </p>
              </div>
            </div>
            <PercentBadge value={data.summary.pnlPercent} className="text-sm" />
            <div className="pt-2 grid grid-cols-2 gap-3">
              <div className="flex items-center gap-2 rounded-lg bg-accent p-3">
                <DollarSign className="h-4 w-4 text-primary" />
                <div>
                  <p className="text-xs text-muted-foreground">Portfolios</p>
                  <p className="text-sm font-semibold">{data.summary.portfolioCount}</p>
                </div>
              </div>
              <div className="flex items-center gap-2 rounded-lg bg-accent p-3">
                <Activity className="h-4 w-4 text-stock-ref" />
                <div>
                  <p className="text-xs text-muted-foreground">Cổ phiếu</p>
                  <p className="text-sm font-semibold">{data.summary.stockCount}</p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Biến động nổi bật</CardTitle>
        </CardHeader>
        <CardContent>
          {hasTopMovers ? (
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-3">
              <TopMoverSection
                title="Top cổ phiếu tăng giá"
                description="Nhóm có phần trăm tăng mạnh nhất"
                stocks={visibleTopGainers}
                icon={TrendingUp}
                iconClassName="text-stock-up"
                emptyText="Chưa có dữ liệu tăng giá."
                highlightByPercent
              />

              <TopMoverSection
                title="Top cổ phiếu giảm giá"
                description="Nhóm có phần trăm giảm mạnh nhất"
                stocks={visibleTopLosers}
                icon={TrendingDown}
                iconClassName="text-stock-down"
                emptyText="Chưa có dữ liệu giảm giá."
                highlightByPercent
              />

              <TopMoverSection
                title="Top khối lượng giao dịch"
                description="Nhóm có thanh khoản bình quân cao"
                stocks={visibleTopMostActive}
                icon={BarChart3}
                iconClassName="text-stock-ref"
                emptyText="Chưa có dữ liệu khối lượng giao dịch."
                highlightByPercent
              />
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">Chưa có dữ liệu biến động.</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
