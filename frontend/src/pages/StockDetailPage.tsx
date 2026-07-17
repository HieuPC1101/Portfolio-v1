import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router-dom";
import { ExternalLink } from "lucide-react";
import { AreaLineChart } from "@/components/common/AreaLineChart";
import { CandlestickChart } from "@/components/common/CandlestickChart";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatStockPrice, parseBackendDate, formatVND } from "@/lib/format";
import { formatISODate } from "@/lib/chartTime";
import {
  getStockDetail,
  getStockNews,
  getStockOverview,
  getStockRatios,
} from "@/repositories/marketRepository";

function isFiniteNumber(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function hasMeaningfulText(value: string | null | undefined): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function formatDateTime(value: string | null): string {
  const parsed = parseBackendDate(value);
  if (!parsed) {
    return "";
  }

  return parsed.toLocaleString("vi-VN");
}

function formatRatio(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) {
    return "--";
  }
  return value.toFixed(2);
}

function formatAsPercent(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) {
    return "--";
  }
  return `${(value * 100).toFixed(2)}%`;
}

function formatCurrencyValue(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) {
    return "--";
  }
  return formatVND(value);
}

function formatInteger(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) {
    return "--";
  }
  return new Intl.NumberFormat("vi-VN").format(value);
}

export default function StockDetailPage() {
  const { symbol: routeSymbol } = useParams();
  const symbol = (routeSymbol ?? "").trim().toUpperCase();

  const { data, isPending, isError } = useQuery({
    queryKey: ["stock-detail", symbol],
    queryFn: () => getStockDetail(symbol),
    enabled: symbol.length > 0,
  });

  const { data: newsItems, isFetching: isNewsFetching } = useQuery({
    queryKey: ["stock-news", symbol],
    queryFn: () => getStockNews(symbol, 5),
    enabled: symbol.length > 0,
  });

  const {
    data: overview,
    isPending: isOverviewPending,
    isError: isOverviewError,
    refetch: refetchOverview,
  } = useQuery({
    queryKey: ["stock-overview", symbol],
    queryFn: () => getStockOverview(symbol),
    enabled: symbol.length > 0,
  });

  const {
    data: ratios,
    isPending: isRatiosPending,
    isError: isRatiosError,
    refetch: refetchRatios,
  } = useQuery({
    queryKey: ["stock-ratios", symbol],
    queryFn: () => getStockRatios(symbol),
    enabled: symbol.length > 0,
  });

  if (!symbol) {
    return <div className="text-sm text-muted-foreground">Thiếu mã cổ phiếu.</div>;
  }

  if (isPending) {
    return (
      <div className="flex h-64 items-center justify-center">
        <LoadingSpinner />
      </div>
    );
  }

  if (isError || !data) {
    return <div className="text-sm text-muted-foreground">Không thể tải dữ liệu cổ phiếu.</div>;
  }

  const latestDate = data.history30d[data.history30d.length - 1]?.date;
  const primaryName = overview?.companyName ?? data.name;
  const overviewSector = overview?.industry ?? overview?.sector ?? data.sector;
  const businessSummary = hasMeaningfulText(overview?.businessSummary) ? overview.businessSummary : null;
  const displayMarketCap = overview?.marketCap
    ?? (isFiniteNumber(overview?.sharesOutstanding) ? overview.sharesOutstanding * data.price * 1_000 : null);

  const ratioItems = [
    { label: "P/E", value: ratios?.pe, formatter: formatRatio },
    { label: "P/B", value: ratios?.pb, formatter: formatRatio },
    { label: "EV/EBITDA", value: ratios?.evEbitda, formatter: formatRatio },
    { label: "Gross Margin", value: ratios?.grossMargin, formatter: formatAsPercent },
    { label: "Net Margin", value: ratios?.netMargin, formatter: formatAsPercent },
    { label: "ROE", value: ratios?.roe, formatter: formatAsPercent },
    { label: "ROA", value: ratios?.roa, formatter: formatAsPercent },
    { label: "D/E", value: ratios?.debtToEquity, formatter: formatRatio },
  ].filter((item) => isFiniteNumber(item.value));

  const sectorCard = hasMeaningfulText(overviewSector)
    ? { label: "Ngành", value: overviewSector }
    : null;
  const exchangeCard = hasMeaningfulText(overview?.exchange)
    ? { label: "Sàn", value: overview.exchange }
    : null;
  const marketCapCard = !isOverviewPending && isFiniteNumber(displayMarketCap)
    ? { label: "Vốn hóa", value: formatCurrencyValue(displayMarketCap) }
    : null;
  const sharesOutstandingCard = isFiniteNumber(overview?.sharesOutstanding)
    ? { label: "Số cổ phiếu lưu hành", value: formatInteger(overview.sharesOutstanding) }
    : null;

  const summaryFactCards = [sectorCard, exchangeCard, marketCapCard, sharesOutstandingCard]
    .filter((item): item is { label: string; value: string } => item !== null);

  const showBusinessSection =
    summaryFactCards.length > 0
    || businessSummary !== null
    || ratioItems.length > 0
    || isOverviewPending
    || isRatiosPending
    || isOverviewError
    || isRatiosError;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">{data.symbol}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{primaryName ?? "Chưa có thông tin doanh nghiệp"}</p>
      </div>

      {showBusinessSection ? (
        <Card className="border-border bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Phân tích doanh nghiệp</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
              <Card className="h-full border-border bg-background/40 shadow-none xl:min-h-[196px]">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">Giá gần nhất</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-2xl font-bold tabular-nums">{formatStockPrice(data.price)}</p>
                  <PercentBadge value={data.percent} />
                  <div className="space-y-1 text-xs text-muted-foreground">
                    <p>{data.price.toLocaleString("vi-VN")} nghìn đ/cp</p>
                    <p>{latestDate ? `Cập nhật: ${formatISODate(latestDate)}` : "Chưa có dữ liệu lịch sử"}</p>
                  </div>
                </CardContent>
              </Card>

              <div className="grid gap-4">
                {sectorCard ? (
                  <Card className="border-border bg-background/40 shadow-none xl:min-h-[90px]">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">{sectorCard.label}</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <p className="text-lg font-semibold">{sectorCard.value}</p>
                    </CardContent>
                  </Card>
                ) : null}

                {exchangeCard ? (
                  <Card className="border-border bg-background/40 shadow-none xl:min-h-[90px]">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">{exchangeCard.label}</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <p className="text-lg font-semibold">{exchangeCard.value}</p>
                    </CardContent>
                  </Card>
                ) : null}
              </div>

              <div className="grid gap-4">
                {marketCapCard ? (
                  <Card className="border-border bg-background/40 shadow-none xl:min-h-[90px]">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">{marketCapCard.label}</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <p className="text-lg font-semibold">{marketCapCard.value}</p>
                    </CardContent>
                  </Card>
                ) : null}

                {sharesOutstandingCard ? (
                  <Card className="border-border bg-background/40 shadow-none xl:min-h-[90px]">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">{sharesOutstandingCard.label}</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <p className="text-lg font-semibold">{sharesOutstandingCard.value}</p>
                    </CardContent>
                  </Card>
                ) : null}
              </div>
            </div>

            {isOverviewPending ? (
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {Array.from({ length: 3 }).map((_, index) => (
                  <Skeleton key={index} className="h-20 w-full" />
                ))}
              </div>
            ) : null}

            {!isOverviewPending && businessSummary ? (
              <div className="rounded-md border border-border p-3">
                <p className="text-xs text-muted-foreground">Mô tả doanh nghiệp</p>
                <p className="mt-2 whitespace-pre-line break-words text-sm leading-relaxed">{businessSummary}</p>
              </div>
            ) : null}

            {!isRatiosPending && ratioItems.length > 0 ? (
              <div className="space-y-3">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                  {ratioItems.map((item) => (
                    <div key={item.label} className="rounded-md border border-border p-3">
                      <p className="text-xs text-muted-foreground">{item.label}</p>
                      <p className="mt-1 text-lg font-semibold">{item.formatter(item.value)}</p>
                    </div>
                  ))}
                </div>

                <div className="space-y-1 text-xs text-muted-foreground">
                  <div>Ngày cập nhật: {ratios?.asOfDate ?? "--"}</div>
                  <div>Kỳ báo cáo: {ratios?.reportingPeriod ?? "--"}</div>
                </div>
              </div>
            ) : null}

            {isRatiosPending ? (
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                <Skeleton className="h-20 w-full" />
                <Skeleton className="h-20 w-full" />
              </div>
            ) : null}

            {isOverviewError && summaryFactCards.length === 0 && businessSummary === null ? (
              <div className="space-y-3">
                <p className="text-sm text-muted-foreground">Không thể tải tổng quan doanh nghiệp.</p>
                <Button variant="outline" size="sm" onClick={() => void refetchOverview()}>Thử lại</Button>
              </div>
            ) : null}

            {isRatiosError && ratioItems.length === 0 ? (
              <div className="space-y-3">
                <p className="text-sm text-muted-foreground">Không thể tải chỉ số phân tích.</p>
                <Button variant="outline" size="sm" onClick={() => void refetchRatios()}>Thử lại</Button>
              </div>
            ) : null}
          </CardContent>
        </Card>
      ) : null}

      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Biến động giá 30 phiên</CardTitle>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="candle" className="space-y-3">
            <TabsList className="bg-accent">
              <TabsTrigger value="candle">Biểu đồ nến</TabsTrigger>
              <TabsTrigger value="area">Đường giá</TabsTrigger>
            </TabsList>

            <TabsContent value="candle" className="mt-0">
              {data.ohlc30d.length > 0 ? (
                <CandlestickChart data={data.ohlc30d} />
              ) : (
                <div className="text-sm text-muted-foreground">Chưa có dữ liệu nến.</div>
              )}
            </TabsContent>

            <TabsContent value="area" className="mt-0">
              {data.history30d.length > 0 ? (
                <AreaLineChart data={data.history30d} />
              ) : (
                <div className="text-sm text-muted-foreground">Chưa có dữ liệu biểu đồ.</div>
              )}
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Tin tức liên quan</CardTitle>
        </CardHeader>
        <CardContent>
          {isNewsFetching ? (
            <div className="text-sm text-muted-foreground">Đang tải tin tức...</div>
          ) : (newsItems ?? []).length === 0 ? (
            <div className="text-sm text-muted-foreground">Chưa có tin tức cho mã này.</div>
          ) : (
            <div className="space-y-3">
              {(newsItems ?? []).map((item, index) => (
                <div key={`${item.title}-${index}`} className="rounded-md border border-border p-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-sm font-semibold leading-snug">{item.title}</p>
                      {item.summary ? <p className="mt-1 text-xs text-muted-foreground">{item.summary}</p> : null}
                      <p className="mt-2 text-xs text-muted-foreground">
                        {item.source}
                        {item.publishedAt ? ` • ${formatDateTime(item.publishedAt)}` : ""}
                      </p>
                    </div>
                    {item.url ? (
                      <a
                        href={item.url}
                        target="_blank"
                        rel="noreferrer"
                        className="shrink-0 rounded p-1 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                      >
                        <ExternalLink className="h-4 w-4" />
                      </a>
                    ) : null}
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
