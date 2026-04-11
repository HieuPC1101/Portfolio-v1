import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { formatVND } from "@/lib/format";
import { useDashboardQuery } from "@/hooks/useDashboardQuery";
import { dashboardMock } from "@/mocks/dashboard.mock";
import type { DashboardData, TopMover } from "@/types/dashboard";
import { Activity, BarChart3, DollarSign, RefreshCw, TrendingDown, TrendingUp } from "lucide-react";
import { Area, Bar, ComposedChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useEffect, useMemo, useState } from "react";
import type { ComponentType } from "react";

const MAX_TOP_MOVER_ROWS = 10;
const TREND_SYMBOL_ORDER = ["VNINDEX", "VN30", "VN30F1M"] as const;

type TrendSymbol = (typeof TREND_SYMBOL_ORDER)[number];

function formatMatchedVolume(value: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return "--";
  }

  return Math.round(value).toLocaleString("vi-VN");
}

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
  const [selectedTrendSymbol, setSelectedTrendSymbol] = useState<TrendSymbol>("VNINDEX");

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setCurrentTime(Date.now());
    }, 1000);

    return () => window.clearInterval(intervalId);
  }, []);

  const trendCards = useMemo(
    () =>
      TREND_SYMBOL_ORDER
        .map((symbol) => {
          const series = (data?.chart ?? [])
            .filter((point) => point.symbol === symbol)
            .sort((a, b) => a.day - b.day);

          if (series.length === 0) {
            return null;
          }

          const latest = series[series.length - 1];
          const previous = series[series.length - 2] ?? latest;
          const change = latest.value - previous.value;
          const changePercent = previous.value !== 0 ? (change / previous.value) * 100 : 0;

          return {
            symbol,
            value: latest.value,
            change,
            changePercent,
            volume: latest.volume,
            series,
          };
        })
        .filter((card): card is NonNullable<typeof card> => card !== null),
    [data?.chart],
  );

  useEffect(() => {
    if (trendCards.length === 0) {
      return;
    }

    const hasSelectedCard = trendCards.some((card) => card.symbol === selectedTrendSymbol);
    if (!hasSelectedCard) {
      setSelectedTrendSymbol(trendCards[0].symbol);
    }
  }, [selectedTrendSymbol, trendCards]);

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

  const selectedTrendCard = trendCards.find((card) => card.symbol === selectedTrendSymbol) ?? trendCards[0] ?? null;
  const selectedTrendChartData = selectedTrendCard
    ? selectedTrendCard.series.map((point) => ({
        ...point,
        matchedVolume: point.volume ?? 0,
      }))
    : [];
  const selectedTrendColor = selectedTrendCard && selectedTrendCard.change < 0 ? "hsl(0, 72%, 63%)" : "hsl(131, 45%, 40%)";

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

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2 bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Diễn biến thị trường</CardTitle>
          </CardHeader>
          <CardContent>
            {trendCards.length > 0 && selectedTrendCard ? (
              <div className="space-y-3">
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                  {trendCards.map((card) => {
                    const isSelected = selectedTrendCard.symbol === card.symbol;
                    const isUp = card.change >= 0;

                    return (
                      <button
                        key={card.symbol}
                        type="button"
                        onClick={() => setSelectedTrendSymbol(card.symbol)}
                        className={`rounded-lg border p-3 text-left transition-colors ${
                          isSelected
                            ? "border-primary/60 bg-primary/10"
                            : "border-border/70 bg-accent/20 hover:bg-accent/40"
                        }`}
                      >
                        <p className="text-xs font-semibold text-muted-foreground">{card.symbol}</p>
                        <p className="mt-1 text-lg font-bold tabular-nums">{card.value.toLocaleString("vi-VN", { maximumFractionDigits: 2 })}</p>
                        <p className={`text-xs tabular-nums ${isUp ? "text-stock-up" : "text-stock-down"}`}>
                          {isUp ? "+" : ""}
                          {card.change.toFixed(2)} ({isUp ? "+" : ""}
                          {card.changePercent.toFixed(2)}%)
                        </p>
                        <p className="mt-1 text-[11px] text-muted-foreground">
                          KL khớp: <span className="tabular-nums text-foreground">{formatMatchedVolume(card.volume)}</span>
                        </p>
                      </button>
                    );
                  })}
                </div>

                <div className="h-[250px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={selectedTrendChartData} margin={{ top: 8, right: 8, left: 0, bottom: 8 }}>
                      <defs>
                        <linearGradient id="trendValueGradient" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor={selectedTrendColor} stopOpacity={0.32} />
                          <stop offset="95%" stopColor={selectedTrendColor} stopOpacity={0} />
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
                      <YAxis yAxisId="price" hide domain={["auto", "auto"]} />
                      <YAxis yAxisId="volume" hide domain={[0, "auto"]} />
                      <Tooltip
                        contentStyle={{
                          background: "hsl(215, 25%, 11%)",
                          border: "1px solid hsl(216, 14%, 22%)",
                          borderRadius: "8px",
                          color: "hsl(213, 27%, 92%)",
                          fontSize: "12px",
                        }}
                        labelFormatter={(label: string) => `Ngày ${label}`}
                        formatter={(value: number, name: string) => {
                          if (name === "KL khớp") {
                            return [formatMatchedVolume(value), "KL khớp"];
                          }
                          return [value.toLocaleString("vi-VN", { maximumFractionDigits: 2 }), selectedTrendCard.symbol];
                        }}
                      />
                      <Bar yAxisId="volume" dataKey="matchedVolume" name="KL khớp" fill="hsl(215, 16%, 52%)" fillOpacity={0.22} radius={[2, 2, 0, 0]} />
                      <Area
                        yAxisId="price"
                        type="monotone"
                        dataKey="value"
                        name={selectedTrendCard.symbol}
                        stroke={selectedTrendColor}
                        strokeWidth={2}
                        fill="url(#trendValueGradient)"
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
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
              {data.summary.pnl !== 0 && (
                <div>
                  <p className="text-xs text-muted-foreground">Lãi/Lỗ</p>
                  <p className={`text-sm font-medium ${data.summary.pnl >= 0 ? "text-stock-up" : "text-stock-down"}`}>
                    {data.summary.pnl >= 0 ? "+" : ""}
                    {formatVND(data.summary.pnl)}
                  </p>
                </div>
              )}
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
