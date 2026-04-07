import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { usePortfolioQuery } from "@/hooks/usePortfolioQuery";
import { formatVND } from "@/lib/format";
import { Plus, BarChart3 } from "lucide-react";
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";

const COLORS = ["hsl(131, 45%, 40%)", "hsl(210, 70%, 55%)", "hsl(40, 65%, 65%)", "hsl(280, 50%, 63%)", "hsl(187, 45%, 55%)"];

export default function PortfolioPage() {
  const { data, isPending, isError } = usePortfolioQuery();

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

  const selected = data.portfolios[0];

  if (!selected) {
    return <div className="text-sm text-muted-foreground">Chưa có danh mục nào.</div>;
  }

  const pieData = selected.holdings.map((h) => ({ name: h.symbol, value: h.weight }));

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Danh mục đầu tư</h1>
          <p className="text-muted-foreground text-sm mt-1">Quản lý các danh mục cổ phiếu</p>
        </div>
        <Button className="gap-2">
          <Plus className="h-4 w-4" /> Tạo danh mục
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {data.portfolios.map((p) => (
          <Card key={p.id} className={`bg-card border-border cursor-pointer transition-colors hover:border-primary/50 ${p.id === selected.id ? "border-primary" : ""}`}>
            <CardContent className="p-4">
              <div className="flex items-start justify-between">
                <div>
                  <h3 className="font-semibold">{p.name}</h3>
                  <p className="text-2xl font-bold mt-1">{formatVND(p.currentValue)}</p>
                </div>
                <PercentBadge value={p.pnlPercent} />
              </div>
              <div className="flex items-center gap-4 mt-3 text-sm text-muted-foreground">
                <span>Đầu tư: {formatVND(p.totalInvested)}</span>
                <span className={p.pnl > 0 ? "text-stock-up" : "text-stock-down"}>
                  {p.pnl > 0 ? "+" : ""}
                  {formatVND(p.pnl)}
                </span>
              </div>
              <div className="flex gap-1 mt-3">
                {p.holdings.map((h) => (
                  <span key={h.symbol} className="text-xs px-2 py-0.5 bg-accent rounded">{h.symbol}</span>
                ))}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2 bg-card border-border">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardTitle className="text-base">{selected.name} — Cổ phiếu</CardTitle>
            <div className="flex gap-2">
              <Button size="sm" variant="outline" className="gap-1 text-xs">
                <BarChart3 className="h-3 w-3" /> Tối ưu hóa
              </Button>
              <Button size="sm" variant="ghost" className="gap-1 text-xs">
                <Plus className="h-3 w-3" /> Thêm
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-muted-foreground text-xs border-b border-border">
                    <th className="text-left py-2 font-medium">Mã</th>
                    <th className="text-right py-2 font-medium">KL</th>
                    <th className="text-right py-2 font-medium">Giá mua</th>
                    <th className="text-right py-2 font-medium">Giá TT</th>
                    <th className="text-right py-2 font-medium">Lãi/Lỗ</th>
                    <th className="text-right py-2 font-medium">%</th>
                  </tr>
                </thead>
                <tbody>
                  {selected.holdings.length > 0 ? (
                    selected.holdings.map((h) => {
                      const pnl = (h.currentPrice - h.avgPrice) * h.shares;
                      const pnlPct = ((h.currentPrice - h.avgPrice) / h.avgPrice) * 100;
                      return (
                        <tr key={h.symbol} className="border-b border-border/50 hover:bg-accent/50">
                          <td className="py-2.5 font-semibold">{h.symbol}</td>
                          <td className="text-right tabular-nums">{h.shares.toLocaleString("vi-VN")}</td>
                          <td className="text-right tabular-nums">{h.avgPrice.toLocaleString("vi-VN")}</td>
                          <td className="text-right tabular-nums">{h.currentPrice.toLocaleString("vi-VN")}</td>
                          <td className={`text-right tabular-nums ${pnl > 0 ? "text-stock-up" : "text-stock-down"}`}>
                            {pnl > 0 ? "+" : ""}
                            {formatVND(pnl)}
                          </td>
                          <td className="text-right">
                            <PercentBadge value={pnlPct} showIcon={false} />
                          </td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td className="py-3 text-sm text-muted-foreground" colSpan={6}>
                        Chưa có cổ phiếu trong danh mục.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Phân bổ danh mục</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[200px]">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={50}
                    outerRadius={80}
                    paddingAngle={3}
                    dataKey="value"
                  >
                    {pieData.map((_, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      background: "hsl(215, 25%, 11%)",
                      border: "1px solid hsl(216, 14%, 22%)",
                      borderRadius: "8px",
                      color: "hsl(213, 27%, 92%)",
                      fontSize: "12px",
                    }}
                    formatter={(value: number) => [`${value}%`, "Tỷ trọng"]}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="space-y-2 mt-2">
              {pieData.map((item, i) => (
                <div key={item.name} className="flex items-center justify-between text-sm">
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: COLORS[i % COLORS.length] }} />
                    <span>{item.name}</span>
                  </div>
                  <span className="tabular-nums text-muted-foreground">{item.value}%</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
