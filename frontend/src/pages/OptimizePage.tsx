import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { usePortfolioQuery } from "@/hooks/usePortfolioQuery";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, AreaChart, Area } from "recharts";
import { Play, X, Zap } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { useOptimizeData } from "@/hooks/useOptimizeData";
import { formatVND } from "@/lib/format";

export default function OptimizePage() {
  const [searchParams] = useSearchParams();
  const { data: portfolioData } = usePortfolioQuery();
  const { data, isPending, isError } = useOptimizeData();
  const [selectedAlgo, setSelectedAlgo] = useState("max_sharpe");
  const [selectedStocks, setSelectedStocks] = useState<string[]>([]);

  const portfolioId = searchParams.get("portfolioId");
  const selectedPortfolio = useMemo(
    () => portfolioData?.portfolios.find((portfolio) => portfolio.id === portfolioId),
    [portfolioData?.portfolios, portfolioId],
  );

  const suggestedStocks = useMemo(
    () => selectedPortfolio?.holdings.map((holding) => holding.symbol) ?? ["VCB", "FPT", "VNM", "HPG", "MWG"],
    [selectedPortfolio],
  );

  useEffect(() => {
    if (selectedPortfolio) {
      setSelectedStocks(suggestedStocks);
    }
  }, [selectedPortfolio, suggestedStocks]);

  const displayStocks = selectedStocks.length > 0 ? selectedStocks : suggestedStocks;

  const algorithms = data?.algorithms ?? [];
  const result = data?.result;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Tối ưu hóa danh mục</h1>
        <p className="text-muted-foreground text-sm mt-1">
          {selectedPortfolio
            ? `Đang tối ưu: ${selectedPortfolio.name}`
            : "Chạy thuật toán phân bổ tài sản tối ưu"}
        </p>
      </div>

      <Card className="bg-card border-border">
        <CardContent className="p-6 space-y-6">
          <div>
            <Label className="text-sm font-semibold mb-3 block">1. Chọn cổ phiếu</Label>
            <div className="flex flex-wrap gap-2 mb-2">
              {displayStocks.map((s) => (
                <span key={s} className="inline-flex items-center gap-1 bg-primary/15 text-primary rounded-full px-3 py-1 text-sm font-medium">
                  {s}
                  <X className="h-3 w-3 cursor-pointer hover:text-destructive" onClick={() => setSelectedStocks((prev) => prev.filter((x) => x !== s))} />
                </span>
              ))}
            </div>
            <Input placeholder="Tìm mã cổ phiếu..." className="bg-background border-border max-w-sm" />
          </div>

          <div>
            <Label className="text-sm font-semibold mb-3 block">2. Chọn thuật toán</Label>
            {algorithms.length > 0 ? (
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                {algorithms.map((algo) => (
                  <button
                    key={algo.id}
                    onClick={() => setSelectedAlgo(algo.id)}
                    className={`text-left p-3 rounded-lg border transition-colors ${
                      selectedAlgo === algo.id ? "border-primary bg-primary/10" : "border-border hover:border-primary/50"
                    }`}
                  >
                    <p className="text-sm font-medium">{algo.name}</p>
                    <p className="text-xs text-muted-foreground mt-0.5">{algo.desc}</p>
                  </button>
                ))}
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">Chưa có danh sách thuật toán.</div>
            )}
          </div>

          <div>
            <Label className="text-sm font-semibold mb-3 block">3. Ngân sách đầu tư</Label>
            <div className="flex gap-3 items-center max-w-sm">
              <Input defaultValue="100,000,000" className="bg-background border-border tabular-nums" />
              <span className="text-sm text-muted-foreground whitespace-nowrap">VND</span>
            </div>
          </div>

          <Button className="gap-2" size="lg">
            <Play className="h-4 w-4" /> Bắt đầu tối ưu hóa
          </Button>
        </CardContent>
      </Card>

      {isPending ? (
        <div className="flex items-center justify-center h-48">
          <LoadingSpinner />
        </div>
      ) : isError ? (
        <div className="text-sm text-muted-foreground">Không thể tải kết quả tối ưu.</div>
      ) : !result ? (
        <div className="text-sm text-muted-foreground">Chưa có kết quả tối ưu để hiển thị.</div>
      ) : (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <Card className="bg-card border-border">
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Phân bổ tỷ trọng</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-[250px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={result.weights} layout="vertical">
                      <XAxis type="number" domain={[0, 40]} tickFormatter={(v) => `${v}%`} tick={{ fill: "hsl(215, 10%, 55%)", fontSize: 12 }} />
                      <YAxis type="category" dataKey="symbol" width={50} tick={{ fill: "hsl(213, 27%, 92%)", fontSize: 12 }} />
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
                      <Bar dataKey="weight" fill="hsl(131, 45%, 40%)" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-card border-border">
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-2">
                  <Zap className="h-4 w-4 text-primary" /> Chỉ số hiệu suất
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid grid-cols-1 gap-4">
                  <div className="flex justify-between items-center p-3 rounded-lg bg-accent">
                    <span className="text-sm text-muted-foreground">Expected Return</span>
                    <span className="text-lg font-bold text-stock-up">+{result.metrics.expectedReturn}%</span>
                  </div>
                  <div className="flex justify-between items-center p-3 rounded-lg bg-accent">
                    <span className="text-sm text-muted-foreground">Volatility</span>
                    <span className="text-lg font-bold">{result.metrics.volatility}%</span>
                  </div>
                  <div className="flex justify-between items-center p-3 rounded-lg bg-accent">
                    <span className="text-sm text-muted-foreground">Sharpe Ratio</span>
                    <span className="text-lg font-bold text-stock-ref">{result.metrics.sharpeRatio}</span>
                  </div>
                </div>
                <Button variant="outline" className="w-full gap-2">
                  <Play className="h-4 w-4" /> Chạy Backtest
                </Button>
              </CardContent>
            </Card>
          </div>

          <Card className="bg-card border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Bảng phân bổ chi tiết</CardTitle>
            </CardHeader>
            <CardContent>
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-muted-foreground text-xs border-b border-border">
                    <th className="text-left py-2 font-medium">Mã</th>
                    <th className="text-right py-2 font-medium">Tỷ trọng</th>
                    <th className="text-right py-2 font-medium">Cổ phiếu</th>
                    <th className="text-right py-2 font-medium">Số tiền</th>
                  </tr>
                </thead>
                <tbody>
                  {result.allocation.map((a) => (
                    <tr key={a.symbol} className="border-b border-border/50">
                      <td className="py-2.5 font-semibold">{a.symbol}</td>
                      <td className="text-right tabular-nums">{a.weight}%</td>
                      <td className="text-right tabular-nums">{a.shares} cp</td>
                      <td className="text-right tabular-nums">{formatVND(a.amount)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>

          <Card className="bg-card border-border">
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Backtest — Equity Curve</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-[300px]">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={result.backtest}>
                    <defs>
                      <linearGradient id="colorPortfolio" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(131, 45%, 40%)" stopOpacity={0.2} />
                        <stop offset="95%" stopColor="hsl(131, 45%, 40%)" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <XAxis dataKey="day" tick={{ fill: "hsl(215, 10%, 55%)", fontSize: 11 }} />
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
                    <Area type="monotone" dataKey="portfolio" stroke="hsl(131, 45%, 40%)" strokeWidth={2} fill="url(#colorPortfolio)" name="Danh mục" />
                    <Area type="monotone" dataKey="benchmark" stroke="hsl(215, 10%, 55%)" strokeWidth={1} fill="none" strokeDasharray="4 4" name="VN-Index" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
