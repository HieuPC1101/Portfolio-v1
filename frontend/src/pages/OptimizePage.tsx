import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Check, ChevronsUpDown, Loader2, Play, Zap } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { usePortfolioQuery } from "@/hooks/usePortfolioQuery";
import { useOptimization, useOptimizationAlgorithms } from "@/hooks/useOptimization";
import type { OptimizeResultData } from "@/types/optimize";
import { formatVND } from "@/lib/format";
import { cn } from "@/lib/utils";
import { validateOptimizationInput } from "@/utils/validators";
import {
  pushRecentPortfolioId,
  readRecentPortfolioIds,
  saveRecentPortfolioIds,
} from "@/utils/recentPortfolios";

const DEFAULT_RISK_FREE_RATE_PERCENT = 5;
const DEFAULT_TARGET_RETURN_PERCENT = 12;

function parsePercentInput(rawValue: string): number | null {
  const normalized = rawValue.replace(/,/g, ".").trim();
  if (!normalized) {
    return null;
  }

  const value = Number(normalized);
  return Number.isFinite(value) ? value : null;
}

function normalizeStocks(stocks: unknown): string[] {
  if (!Array.isArray(stocks)) {
    return [];
  }

  const normalized = stocks
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);

  return Array.from(new Set(normalized));
}

export default function OptimizePage() {
  const [searchParams] = useSearchParams();

  const {
    data: portfolioData,
    isPending: isLoadingPortfolios,
    isError: hasPortfolioError,
  } = usePortfolioQuery();
  const {
    data: algorithms,
    isPending: isLoadingAlgorithms,
    isError: hasAlgorithmsError,
  } = useOptimizationAlgorithms();
  const {
    mutateAsync: optimizeAsync,
    isPending: isOptimizing,
    error: optimizeError,
  } = useOptimization();

  const portfolioId = searchParams.get("portfolioId");
  const [selectedPortfolioId, setSelectedPortfolioId] = useState<string | null>(() => portfolioId);
  const [isPortfolioPickerOpen, setIsPortfolioPickerOpen] = useState(false);
  const [recentPortfolioIds, setRecentPortfolioIds] = useState<string[]>([]);
  const [selectedAlgo, setSelectedAlgo] = useState("max_sharpe");
  const [riskFreeRateInput, setRiskFreeRateInput] = useState(String(DEFAULT_RISK_FREE_RATE_PERCENT));
  const [targetReturnInput, setTargetReturnInput] = useState(String(DEFAULT_TARGET_RETURN_PERCENT));
  const [submitErrors, setSubmitErrors] = useState<ReturnType<typeof validateOptimizationInput>>({});
  const [result, setResult] = useState<OptimizeResultData | null>(null);

  const portfolios = portfolioData?.portfolios ?? [];
  const selectedPortfolio = useMemo(
    () => portfolios.find((portfolio) => portfolio.id === selectedPortfolioId) ?? null,
    [portfolios, selectedPortfolioId],
  );
  const selectedStocks = useMemo(
    () => normalizeStocks(selectedPortfolio?.holdings.map((holding) => holding.symbol) ?? []),
    [selectedPortfolio],
  );
  const recentPortfolios = useMemo(
    () => recentPortfolioIds
      .map((id) => portfolios.find((portfolio) => portfolio.id === id))
      .filter((portfolio): portfolio is NonNullable<typeof portfolio> => Boolean(portfolio)),
    [portfolios, recentPortfolioIds],
  );
  const nonRecentPortfolios = useMemo(
    () => portfolios.filter((portfolio) => !recentPortfolioIds.includes(portfolio.id)),
    [portfolios, recentPortfolioIds],
  );
  const portfolioBudget = selectedPortfolio?.totalInvested ?? 0;

  useEffect(() => {
    setRecentPortfolioIds(readRecentPortfolioIds());
  }, []);

  useEffect(() => {
    if (portfolioId) {
      setSelectedPortfolioId(portfolioId);
    }
  }, [portfolioId]);

  useEffect(() => {
    if (portfolios.length === 0) {
      setSelectedPortfolioId(null);
      return;
    }

    if (selectedPortfolioId && portfolios.some((portfolio) => portfolio.id === selectedPortfolioId)) {
      return;
    }

    if (portfolioId && portfolios.some((portfolio) => portfolio.id === portfolioId)) {
      setSelectedPortfolioId(portfolioId);
      return;
    }

    setSelectedPortfolioId(portfolios[0].id);
  }, [portfolioId, portfolios, selectedPortfolioId]);

  useEffect(() => {
    if (!selectedPortfolioId || !portfolios.some((portfolio) => portfolio.id === selectedPortfolioId)) {
      return;
    }

    setRecentPortfolioIds(pushRecentPortfolioId(selectedPortfolioId));
  }, [portfolios, selectedPortfolioId]);

  useEffect(() => {
    if (portfolios.length === 0) {
      if (recentPortfolioIds.length > 0) {
        setRecentPortfolioIds([]);
        saveRecentPortfolioIds([]);
      }
      return;
    }

    const validIds = recentPortfolioIds.filter((id) => portfolios.some((portfolio) => portfolio.id === id));
    if (validIds.length !== recentPortfolioIds.length) {
      setRecentPortfolioIds(validIds);
      saveRecentPortfolioIds(validIds);
    }
  }, [portfolios, recentPortfolioIds]);

  useEffect(() => {
    if (!selectedAlgo && algorithms && algorithms.length > 0) {
      setSelectedAlgo(algorithms[0].id);
    }
  }, [algorithms, selectedAlgo]);

  const riskFreeRateValue = parsePercentInput(riskFreeRateInput);
  const targetReturnValue = parsePercentInput(targetReturnInput);
  const algorithmParameterConfig = useMemo(() => {
    if (selectedAlgo === "markowitz") {
      return {
        label: "3. Target return tối thiểu",
        description: "Markowitz sẽ tối thiểu hóa phương sai với ràng buộc lợi nhuận kỳ vọng >= target return.",
        value: targetReturnInput,
        placeholder: "Ví dụ: 12",
        suffix: "%",
        onChange: setTargetReturnInput,
      };
    }

    if (selectedAlgo === "max_sharpe") {
      return {
        label: "3. Risk-free rate",
        description: "Max Sharpe sẽ tối đa hóa Sharpe ratio theo mức lãi suất phi rủi ro bạn nhập.",
        value: riskFreeRateInput,
        placeholder: "Ví dụ: 5",
        suffix: "%",
        onChange: setRiskFreeRateInput,
      };
    }

    return null;
  }, [riskFreeRateInput, selectedAlgo, targetReturnInput]);

  const maxWeight = useMemo(() => {
    if (!result?.weights.length) {
      return 40;
    }

    const topWeight = Math.max(...result.weights.map((item) => item.weight));
    return Math.max(10, Math.ceil(topWeight / 5) * 5);
  }, [result?.weights]);

  async function runOptimization() {
    const errors = validateOptimizationInput({
      selectedStocks,
      algorithm: selectedAlgo,
      budget: portfolioBudget,
      algorithmParameter: selectedAlgo === "markowitz"
        ? targetReturnValue
        : selectedAlgo === "max_sharpe"
          ? riskFreeRateValue
          : undefined,
    });

    setSubmitErrors(errors);

    if (Object.keys(errors).length > 0) {
      return;
    }

    const optimizationResult = await optimizeAsync({
      stocks: selectedStocks,
      algorithm: selectedAlgo,
      budget: portfolioBudget,
      constraints: {
        riskFreeRate: riskFreeRateValue !== null ? riskFreeRateValue / 100 : undefined,
        targetReturn: targetReturnValue !== null ? targetReturnValue / 100 : null,
        maxWeight: 0.4,
        minWeight: 0.05,
      },
      portfolioId: selectedPortfolio ? Number(selectedPortfolio.id) : undefined,
    });

    setResult(optimizationResult);
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Tối ưu hóa danh mục</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {selectedPortfolio
            ? `Đang tối ưu: ${selectedPortfolio.name}`
            : "Chạy thuật toán phân bổ tài sản tối ưu"}
        </p>
      </div>

      <Card className="border-border bg-card">
        <CardContent className="space-y-6 p-6">
          <div>
            <Label className="mb-3 block text-sm font-semibold">1. Chọn danh mục</Label>
            {isLoadingPortfolios ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" /> Đang tải danh mục...
              </div>
            ) : hasPortfolioError ? (
              <div className="text-sm text-muted-foreground">Không tải được danh mục đầu tư.</div>
            ) : portfolios.length === 0 ? (
              <div className="text-sm text-muted-foreground">Chưa có danh mục. Vui lòng tạo danh mục trước khi tối ưu.</div>
            ) : (
              <>
                <div className="mb-4 max-w-lg">
                  <Popover open={isPortfolioPickerOpen} onOpenChange={setIsPortfolioPickerOpen}>
                    <PopoverTrigger asChild>
                      <Button
                        type="button"
                        variant="outline"
                        role="combobox"
                        aria-expanded={isPortfolioPickerOpen}
                        className="w-full justify-between"
                      >
                        <span className="truncate">
                          {selectedPortfolio ? selectedPortfolio.name : "Chọn danh mục để tối ưu hóa"}
                        </span>
                        <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-[var(--radix-popover-trigger-width)] p-0" align="start">
                      <Command>
                        <CommandInput placeholder="Tìm danh mục..." />
                        <CommandList>
                          <CommandEmpty>Không tìm thấy danh mục phù hợp.</CommandEmpty>
                          {recentPortfolios.length > 0 && (
                            <CommandGroup heading="Danh mục gần đây">
                              {recentPortfolios.map((portfolio) => (
                                <CommandItem
                                  key={`recent-${portfolio.id}`}
                                  value={`${portfolio.name} ${portfolio.id}`}
                                  onSelect={() => {
                                    setSelectedPortfolioId(portfolio.id);
                                    setIsPortfolioPickerOpen(false);
                                  }}
                                >
                                  <Check
                                    className={cn(
                                      "mr-2 h-4 w-4",
                                      selectedPortfolioId === portfolio.id ? "opacity-100" : "opacity-0",
                                    )}
                                  />
                                  <span className="flex-1 truncate">{portfolio.name}</span>
                                  <span className="text-xs text-muted-foreground">{portfolio.holdings.length} mã</span>
                                </CommandItem>
                              ))}
                            </CommandGroup>
                          )}

                          <CommandGroup heading="Tất cả danh mục">
                            {nonRecentPortfolios.map((portfolio) => (
                              <CommandItem
                                key={portfolio.id}
                                value={`${portfolio.name} ${portfolio.id}`}
                                onSelect={() => {
                                  setSelectedPortfolioId(portfolio.id);
                                  setIsPortfolioPickerOpen(false);
                                }}
                              >
                                <Check
                                  className={cn(
                                    "mr-2 h-4 w-4",
                                    selectedPortfolioId === portfolio.id ? "opacity-100" : "opacity-0",
                                  )}
                                />
                                <span className="flex-1 truncate">{portfolio.name}</span>
                                <span className="text-xs text-muted-foreground">{portfolio.holdings.length} mã</span>
                              </CommandItem>
                            ))}
                          </CommandGroup>
                        </CommandList>
                      </Command>
                    </PopoverContent>
                  </Popover>
                </div>

                <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_260px]">
                  <div className="rounded-lg border border-border bg-background/30 p-4">
                    <p className="text-sm text-muted-foreground">
                      {selectedPortfolio
                        ? `Đang dùng ${selectedPortfolio.name} với ${selectedStocks.length} mã để tối ưu hóa.`
                        : "Vui lòng chọn danh mục để tiếp tục."}
                    </p>

                    {selectedStocks.length > 0 ? (
                      <div className="mt-4 flex flex-wrap gap-2.5">
                        {selectedStocks.map((symbol) => (
                          <span
                            key={symbol}
                            className="inline-flex items-center rounded-full bg-primary/15 px-3 py-1.5 text-sm font-medium text-primary"
                          >
                            {symbol}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-3 text-sm text-muted-foreground">Danh mục chưa có mã cổ phiếu.</p>
                    )}
                  </div>

                  <div className="grid gap-2.5 sm:grid-cols-2 lg:grid-cols-1">
                    <div className="rounded-lg border border-border/70 bg-accent/40 p-3.5">
                      <p className="text-xs uppercase tracking-wide text-muted-foreground">Số mã tối ưu</p>
                      <p className="mt-1 text-lg font-semibold">{selectedStocks.length} mã</p>
                    </div>
                    <div className="rounded-lg border border-border/70 bg-accent/40 p-3.5">
                      <p className="text-xs uppercase tracking-wide text-muted-foreground">Ngân sách danh mục</p>
                      <p className="mt-1 text-lg font-semibold">{formatVND(portfolioBudget)}</p>
                    </div>
                  </div>
                </div>
              </>
            )}
            {submitErrors.stocks && <p className="mt-2 text-xs text-destructive">{submitErrors.stocks}</p>}
          </div>

          <div>
            <Label className="mb-3 block text-sm font-semibold">2. Chọn thuật toán</Label>
            {isLoadingAlgorithms ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" /> Đang tải thuật toán...
              </div>
            ) : hasAlgorithmsError ? (
              <div className="text-sm text-muted-foreground">Không tải được danh sách thuật toán.</div>
            ) : !algorithms || algorithms.length === 0 ? (
              <div className="text-sm text-muted-foreground">Chưa có danh sách thuật toán.</div>
            ) : (
              <div className="grid grid-cols-1 gap-2 md:grid-cols-2 lg:grid-cols-3">
                {algorithms.map((algo) => (
                  <button
                    key={algo.id}
                    type="button"
                    onClick={() => setSelectedAlgo(algo.id)}
                    className={`rounded-lg border p-3 text-left transition-colors ${
                      selectedAlgo === algo.id
                        ? "border-primary bg-primary/10"
                        : "border-border hover:border-primary/50"
                    }`}
                  >
                    <p className="text-sm font-medium">{algo.name}</p>
                    <p className="mt-0.5 text-xs text-muted-foreground">{algo.desc}</p>
                  </button>
                ))}
              </div>
            )}
            {submitErrors.algorithm && <p className="mt-2 text-xs text-destructive">{submitErrors.algorithm}</p>}
          </div>

          {algorithmParameterConfig && (
            <div>
              <Label className="mb-3 block text-sm font-semibold">{algorithmParameterConfig.label}</Label>
              <div className="flex max-w-sm items-center gap-3">
                <Input
                  value={algorithmParameterConfig.value}
                  placeholder={algorithmParameterConfig.placeholder}
                  className="tabular-nums"
                  onChange={(event) => algorithmParameterConfig.onChange(event.target.value)}
                />
                <span className="whitespace-nowrap text-sm text-muted-foreground">
                  {algorithmParameterConfig.suffix}
                </span>
              </div>
              <p className="mt-2 text-xs text-muted-foreground">{algorithmParameterConfig.description}</p>
              {submitErrors.algorithmParameter && (
                <p className="mt-2 text-xs text-destructive">{submitErrors.algorithmParameter}</p>
              )}
            </div>
          )}

          {submitErrors.budget && <p className="text-xs text-destructive">{submitErrors.budget}</p>}

          <Button className="gap-2" size="lg" disabled={isOptimizing} onClick={() => void runOptimization()}>
            {isOptimizing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            {isOptimizing ? "Đang tối ưu hóa..." : "Bắt đầu tối ưu hóa"}
          </Button>

          {optimizeError && (
            <div className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
              {optimizeError instanceof Error ? optimizeError.message : "Không thể tối ưu hóa danh mục"}
            </div>
          )}
        </CardContent>
      </Card>

      {isOptimizing ? (
        <div className="flex h-48 items-center justify-center">
          <LoadingSpinner />
        </div>
      ) : !result ? (
        <div className="text-sm text-muted-foreground">Chưa có kết quả tối ưu để hiển thị.</div>
      ) : (
        <>
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <Card className="border-border bg-card">
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Phân bổ tỷ trọng</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-[250px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={result.weights} layout="vertical">
                      <XAxis
                        type="number"
                        domain={[0, maxWeight]}
                        tickFormatter={(value) => `${value}%`}
                        tick={{ fill: "hsl(215, 10%, 55%)", fontSize: 12 }}
                      />
                      <YAxis
                        type="category"
                        dataKey="symbol"
                        width={50}
                        tick={{ fill: "hsl(213, 27%, 92%)", fontSize: 12 }}
                      />
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

            <Card className="border-border bg-card">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Zap className="h-4 w-4 text-primary" /> Chỉ số hiệu suất
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between rounded-lg bg-accent p-3">
                  <span className="text-sm text-muted-foreground">Expected Return</span>
                  <span className="text-lg font-bold text-stock-up">+{result.metrics.expectedReturn}%</span>
                </div>
                <div className="flex items-center justify-between rounded-lg bg-accent p-3">
                  <span className="text-sm text-muted-foreground">Volatility</span>
                  <span className="text-lg font-bold">{result.metrics.volatility}%</span>
                </div>
                <div className="flex items-center justify-between rounded-lg bg-accent p-3">
                  <span className="text-sm text-muted-foreground">Sharpe Ratio</span>
                  <span className="text-lg font-bold text-stock-ref">{result.metrics.sharpeRatio}</span>
                </div>
                {typeof result.metrics.maxDrawdown === "number" && (
                  <div className="flex items-center justify-between rounded-lg bg-accent p-3">
                    <span className="text-sm text-muted-foreground">Max Drawdown</span>
                    <span className="text-lg font-bold">{result.metrics.maxDrawdown}%</span>
                  </div>
                )}
                {typeof result.metrics.cvar === "number" && (
                  <div className="flex items-center justify-between rounded-lg bg-accent p-3">
                    <span className="text-sm text-muted-foreground">CVaR</span>
                    <span className="text-lg font-bold">{result.metrics.cvar}%</span>
                  </div>
                )}
                {typeof result.metrics.cdar === "number" && (
                  <div className="flex items-center justify-between rounded-lg bg-accent p-3">
                    <span className="text-sm text-muted-foreground">CDaR</span>
                    <span className="text-lg font-bold">{result.metrics.cdar}%</span>
                  </div>
                )}
                {typeof result.metrics.beta === "number" && (
                  <div className="flex items-center justify-between rounded-lg bg-accent p-3">
                    <span className="text-sm text-muted-foreground">Beta</span>
                    <span className="text-lg font-bold">{result.metrics.beta}</span>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          <Card className="border-border bg-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Bảng phân bổ chi tiết</CardTitle>
            </CardHeader>
            <CardContent>
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-xs text-muted-foreground">
                    <th className="py-2 text-left font-medium">Mã</th>
                    <th className="py-2 text-right font-medium">Tỷ trọng</th>
                    <th className="py-2 text-right font-medium">Cổ phiếu</th>
                    <th className="py-2 text-right font-medium">Số tiền</th>
                  </tr>
                </thead>
                <tbody>
                  {result.allocation.map((item) => (
                    <tr key={item.symbol} className="border-b border-border/50">
                      <td className="py-2.5 font-semibold">{item.symbol}</td>
                      <td className="text-right tabular-nums">{item.weight}%</td>
                      <td className="text-right tabular-nums">{item.shares} cp</td>
                      <td className="text-right tabular-nums">{formatVND(item.amount)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>

          {result.backtest.length > 0 && (
            <Card className="border-border bg-card">
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Backtest - Equity Curve</CardTitle>
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
                      <Area
                        type="monotone"
                        dataKey="portfolio"
                        stroke="hsl(131, 45%, 40%)"
                        strokeWidth={2}
                        fill="url(#colorPortfolio)"
                        name="Danh mục"
                      />
                      <Area
                        type="monotone"
                        dataKey="benchmark"
                        stroke="hsl(215, 10%, 55%)"
                        strokeWidth={1}
                        fill="none"
                        strokeDasharray="4 4"
                        name="VN-Index"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
