import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { ForeignFlowPoint, MarketData } from "@/types/market";
import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

function FlowSeriesChart({
  data,
  dataKey,
  color,
  label,
}: {
  data: ForeignFlowPoint[];
  dataKey: "buy" | "sell";
  color: string;
  label: string;
}) {
  return (
    <div className="h-[220px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 8 }}>
          <XAxis
            dataKey="day"
            axisLine={{ stroke: "hsl(216, 14%, 22%)" }}
            tickLine={false}
            tick={{ fill: "hsl(215, 12%, 65%)", fontSize: 11 }}
          />
          <YAxis tick={{ fill: "hsl(215, 12%, 65%)", fontSize: 11 }} width={44} />
          <Tooltip
            contentStyle={{
              background: "hsl(215, 25%, 11%)",
              border: "1px solid hsl(216, 14%, 22%)",
              borderRadius: "8px",
              color: "hsl(213, 27%, 92%)",
              fontSize: "12px",
            }}
            labelFormatter={(value) => `Phiên ${value}`}
            formatter={(value: number) => [value.toLocaleString("vi-VN"), label]}
          />
          <Area
            type="monotone"
            dataKey={dataKey}
            stroke={color}
            fill={color}
            fillOpacity={0.2}
            strokeWidth={2}
            name={label}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function ForeignFlowSeriesTabs({ data }: { data: ForeignFlowPoint[] }) {
  if (data.length === 0) {
    return <div className="text-sm text-muted-foreground">Không có dữ liệu cho khung thời gian này.</div>;
  }

  return (
    <Tabs defaultValue="buy" className="space-y-3">
      <TabsList className="bg-accent">
        <TabsTrigger value="buy">Mua ròng</TabsTrigger>
        <TabsTrigger value="sell">Bán ròng</TabsTrigger>
      </TabsList>

      <TabsContent value="buy" className="mt-0">
        <FlowSeriesChart data={data} dataKey="buy" color="hsl(131, 45%, 40%)" label="Mua ròng" />
      </TabsContent>

      <TabsContent value="sell" className="mt-0">
        <FlowSeriesChart data={data} dataKey="sell" color="hsl(0, 72%, 63%)" label="Bán ròng" />
      </TabsContent>
    </Tabs>
  );
}

interface MarketFlowTabProps {
  data: MarketData;
}

export function MarketFlowTab({ data }: MarketFlowTabProps) {
  if (data.foreignFlow.length === 0) {
    return (
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Dòng tiền khối ngoại (tỷ VND)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-sm text-muted-foreground">Chưa có dữ liệu khối ngoại.</div>
        </CardContent>
      </Card>
    );
  }

  const flow5Sessions = data.foreignFlow.slice(-5);
  const flow1Month = data.foreignFlow.slice(-20);

  return (
    <Card className="border-border bg-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-base">Dòng tiền khối ngoại (tỷ VND)</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <Tabs defaultValue="sessions-5" className="space-y-3">
          <TabsList className="bg-accent">
            <TabsTrigger value="sessions-5">5 phiên</TabsTrigger>
            <TabsTrigger value="month-1">1 tháng</TabsTrigger>
          </TabsList>

          <TabsContent value="sessions-5" className="mt-0">
            <ForeignFlowSeriesTabs data={flow5Sessions} />
          </TabsContent>

          <TabsContent value="month-1" className="mt-0">
            <ForeignFlowSeriesTabs data={flow1Month} />
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}
