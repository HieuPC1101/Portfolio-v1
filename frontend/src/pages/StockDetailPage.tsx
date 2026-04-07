import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router-dom";
import { ExternalLink } from "lucide-react";
import { AreaLineChart } from "@/components/common/AreaLineChart";
import { CandlestickChart } from "@/components/common/CandlestickChart";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatStockPrice } from "@/lib/format";
import { formatISODate } from "@/lib/chartTime";
import { getStockDetail, getStockNews } from "@/repositories/marketRepository";

function formatDateTime(value: string | null): string {
  if (!value) {
    return "";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  return parsed.toLocaleString("vi-VN");
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

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">{data.symbol}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{data.name ?? "Chưa có thông tin doanh nghiệp"}</p>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card className="border-border bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Giá gần nhất</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold tabular-nums">{formatStockPrice(data.price)}</p>
            <p className="mt-0.5 text-xs text-muted-foreground">{data.price.toLocaleString("vi-VN")} nghìn đ/cp</p>
            <p className="mt-1 text-xs text-muted-foreground">{latestDate ? `Cập nhật: ${formatISODate(latestDate)}` : "Chưa có dữ liệu lịch sử"}</p>
          </CardContent>
        </Card>

        <Card className="border-border bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Biến động ngày</CardTitle>
          </CardHeader>
          <CardContent>
            <PercentBadge value={data.percent} />
          </CardContent>
        </Card>

        <Card className="border-border bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Sàn giao dịch</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-lg font-semibold">{data.exchange ?? "N/A"}</p>
          </CardContent>
        </Card>

        <Card className="border-border bg-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Ngành</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-lg font-semibold">{data.sector ?? "N/A"}</p>
          </CardContent>
        </Card>
      </div>

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
