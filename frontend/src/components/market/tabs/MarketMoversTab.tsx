import { PercentBadge } from "@/components/common/PercentBadge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { MarketData, StockBasic } from "@/types/market";

function MoversList({ stocks, emptyText }: { stocks: StockBasic[]; emptyText: string }) {
  if (stocks.length === 0) {
    return <div className="text-sm text-muted-foreground">{emptyText}</div>;
  }

  return (
    <div className="space-y-2">
      {stocks.map((stock, index) => (
        <div
          key={stock.symbol}
          className="flex items-center justify-between rounded-lg border border-border/70 px-3 py-2 hover:bg-accent/50"
        >
          <div className="flex items-center gap-2">
            <span className="w-5 text-xs text-muted-foreground">{index + 1}</span>
            <span className="font-semibold">{stock.symbol}</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-sm tabular-nums">{stock.price.toLocaleString("vi-VN")}</span>
            <PercentBadge value={stock.percent} />
          </div>
        </div>
      ))}
    </div>
  );
}

interface MarketMoversTabProps {
  data: MarketData;
}

export function MarketMoversTab({ data }: MarketMoversTabProps) {
  return (
    <Card className="border-border bg-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-base">Biến động cổ phiếu</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <Tabs defaultValue="gainers" className="space-y-3">
          <TabsList className="bg-accent">
            <TabsTrigger value="gainers">Tăng mạnh</TabsTrigger>
            <TabsTrigger value="losers">Giảm mạnh</TabsTrigger>
          </TabsList>

          <TabsContent value="gainers" className="mt-0">
            <MoversList stocks={data.topGainers} emptyText="Chưa có dữ liệu tăng giá." />
          </TabsContent>

          <TabsContent value="losers" className="mt-0">
            <MoversList stocks={data.topLosers} emptyText="Chưa có dữ liệu giảm giá." />
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}
