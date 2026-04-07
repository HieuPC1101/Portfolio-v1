import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useMarketQuery } from "@/hooks/useMarketQuery";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

export default function MarketPage() {
  const { data, isPending, isError } = useMarketQuery();

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
      <div>
        <h1 className="text-2xl font-bold">Thị trường</h1>
        <p className="text-muted-foreground text-sm mt-1">Tổng quan thị trường chứng khoán Việt Nam</p>
      </div>

      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Bản đồ ngành</CardTitle>
        </CardHeader>
        <CardContent>
          {data.sectors.length > 0 ? (
            <div className="grid grid-cols-4 md:grid-cols-7 gap-1.5">
              {data.sectors.map((s) => (
                <div
                  key={s.name}
                  className="rounded-lg p-3 text-center"
                  style={{
                    backgroundColor:
                      s.change > 0
                        ? `hsl(131, 45%, ${30 + s.change * 5}%)`
                        : `hsl(0, 60%, ${35 + Math.abs(s.change) * 5}%)`,
                    gridColumn: s.size > 20 ? "span 2" : undefined,
                  }}
                >
                  <p className="text-xs font-medium text-foreground">{s.name}</p>
                  <p className="text-xs tabular-nums text-foreground/80 mt-0.5">
                    {s.change > 0 ? "+" : ""}
                    {s.change}%
                  </p>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">Chưa có dữ liệu ngành.</div>
          )}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <Tabs defaultValue="gainers">
              <TabsList className="bg-accent">
                <TabsTrigger value="gainers">Tăng mạnh</TabsTrigger>
                <TabsTrigger value="losers">Giảm mạnh</TabsTrigger>
              </TabsList>
              <TabsContent value="gainers" className="mt-3">
                {data.topGainers.length > 0 ? (
                  <div className="space-y-2">
                    {data.topGainers.map((s) => (
                      <div key={s.symbol} className="flex items-center justify-between p-2 rounded-lg hover:bg-accent/50">
                        <span className="font-semibold">{s.symbol}</span>
                        <div className="flex items-center gap-3">
                          <span className="tabular-nums text-sm">{s.price.toLocaleString("vi-VN")}</span>
                          <PercentBadge value={s.percent} />
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">Chưa có dữ liệu tăng giá.</div>
                )}
              </TabsContent>
              <TabsContent value="losers" className="mt-3">
                {data.topLosers.length > 0 ? (
                  <div className="space-y-2">
                    {data.topLosers.map((s) => (
                      <div key={s.symbol} className="flex items-center justify-between p-2 rounded-lg hover:bg-accent/50">
                        <span className="font-semibold">{s.symbol}</span>
                        <div className="flex items-center gap-3">
                          <span className="tabular-nums text-sm">{s.price.toLocaleString("vi-VN")}</span>
                          <PercentBadge value={s.percent} />
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">Chưa có dữ liệu giảm giá.</div>
                )}
              </TabsContent>
            </Tabs>
          </CardHeader>
        </Card>

        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Giao dịch nước ngoài (tỷ VND)</CardTitle>
          </CardHeader>
          <CardContent>
            {data.foreignFlow.length > 0 ? (
              <div className="h-[200px]">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={data.foreignFlow}>
                    <XAxis dataKey="day" tick={{ fill: "hsl(215, 10%, 55%)", fontSize: 12 }} />
                    <YAxis tick={{ fill: "hsl(215, 10%, 55%)", fontSize: 11 }} />
                    <Tooltip
                      contentStyle={{
                        background: "hsl(215, 25%, 11%)",
                        border: "1px solid hsl(216, 14%, 22%)",
                        borderRadius: "8px",
                        color: "hsl(213, 27%, 92%)",
                        fontSize: "12px",
                      }}
                    />
                    <Area
                      type="monotone"
                      dataKey="buy"
                      stroke="hsl(131, 45%, 40%)"
                      fill="hsl(131, 45%, 40%)"
                      fillOpacity={0.2}
                      name="Mua"
                    />
                    <Area
                      type="monotone"
                      dataKey="sell"
                      stroke="hsl(0, 72%, 63%)"
                      fill="hsl(0, 72%, 63%)"
                      fillOpacity={0.2}
                      name="Bán"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">Chưa có dữ liệu khối ngoại.</div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
