import { useEffect, useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  BarChart3,
  Check,
  ChevronsUpDown,
  ChevronDown,
  Loader2,
  MoreVertical,
  Pencil,
  PieChart as PieChartIcon,
  Plus,
  Trash2,
} from "lucide-react";
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { usePortfolioQuery } from "@/hooks/usePortfolioQuery";
import { usePortfolioSuggestions } from "@/hooks/usePortfolioSuggestions";
import { useSimilarStocks } from "@/hooks/useSimilarStocks";
import { useAddStock } from "@/hooks/useAddStock";
import { useCreatePortfolio } from "@/hooks/useCreatePortfolio";
import { formatVND } from "@/lib/format";
import { cn } from "@/lib/utils";
import { createClientNotification } from "@/repositories/notificationRepository";
import type { PortfolioHolding, PortfolioItem } from "@/types/portfolio";
import type { StockCatalogItem, SystemPortfolioPreset } from "@/types/portfolioSuggestion";
import {
  pushRecentPortfolioId,
  readRecentPortfolioIds,
  saveRecentPortfolioIds,
} from "@/utils/recentPortfolios";
import { AddStockDialog } from "@/components/portfolio/AddStockDialog";
import { CreatePortfolioDialog } from "@/components/portfolio/CreatePortfolioDialog";
import { DeletePortfolioDialog } from "@/components/portfolio/DeletePortfolioDialog";
import { DeleteStockDialog } from "@/components/portfolio/DeleteStockDialog";
import { EditPortfolioDialog } from "@/components/portfolio/EditPortfolioDialog";
import { EditStockDialog } from "@/components/portfolio/EditStockDialog";
import { showPortfolioMiniToast } from "@/components/notifications/PortfolioMiniToast";

const COLORS = ["hsl(131, 45%, 40%)", "hsl(210, 70%, 55%)", "hsl(40, 65%, 65%)", "hsl(280, 50%, 63%)", "hsl(187, 45%, 55%)"];
const CATALOG_PAGE_SIZE = 20;
const LOCALE_NUMBER_FORMATTER = new Intl.NumberFormat("vi-VN");
const LOCALE_DECIMAL_FORMATTER = new Intl.NumberFormat("vi-VN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

type CatalogSortKey =
  | "symbol-asc"
  | "symbol-desc"
  | "price-desc"
  | "price-asc"
  | "change-desc"
  | "change-asc"
  | "volume-desc";

function parseOptionalNumber(value: string): number | null {
  const normalized = value.trim();
  if (!normalized) {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function toCompactMarketCap(value: number): string {
  if (value >= 1_000) {
    return `${new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 1 }).format(value / 1_000)} nghìn tỷ`;
  }

  return `${LOCALE_NUMBER_FORMATTER.format(value)} tỷ`;
}

function formatLocaleNumber(value: number): string {
  return LOCALE_NUMBER_FORMATTER.format(value);
}

function formatLocalePercent(value: number): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}${LOCALE_DECIMAL_FORMATTER.format(value)}%`;
}

export default function PortfolioPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data, isPending, isError } = usePortfolioQuery();
  const { data: suggestionData, isPending: isSuggestionPending } = usePortfolioSuggestions();
  const addStockMutation = useAddStock();
  const createPortfolioMutation = useCreatePortfolio();

  const [selectedPortfolioId, setSelectedPortfolioId] = useState<string | null>(null);
  const [isEditPortfolioOpen, setIsEditPortfolioOpen] = useState(false);
  const [isDeletePortfolioOpen, setIsDeletePortfolioOpen] = useState(false);
  const [isAddStockOpen, setIsAddStockOpen] = useState(false);
  const [isEditStockOpen, setIsEditStockOpen] = useState(false);
  const [isDeleteStockOpen, setIsDeleteStockOpen] = useState(false);
  const [editingPortfolio, setEditingPortfolio] = useState<PortfolioItem | null>(null);
  const [deletingPortfolio, setDeletingPortfolio] = useState<PortfolioItem | null>(null);
  const [editingStock, setEditingStock] = useState<PortfolioHolding | null>(null);
  const [deletingStock, setDeletingStock] = useState<PortfolioHolding | null>(null);

  const [catalogQuery, setCatalogQuery] = useState("");
  const [catalogExchange, setCatalogExchange] = useState("ALL");
  const [catalogSector, setCatalogSector] = useState("ALL");
  const [catalogPriceMin, setCatalogPriceMin] = useState("");
  const [catalogPriceMax, setCatalogPriceMax] = useState("");
  const [catalogChangeMin, setCatalogChangeMin] = useState("");
  const [catalogChangeMax, setCatalogChangeMax] = useState("");
  const [catalogSort, setCatalogSort] = useState<CatalogSortKey>("symbol-asc");
  const [visibleCatalogCount, setVisibleCatalogCount] = useState(CATALOG_PAGE_SIZE);
  const [activePortfolioTab, setActivePortfolioTab] = useState("discover");
  const [activeTrendingTab, setActiveTrendingTab] = useState("");
  const [similarSourceSymbol, setSimilarSourceSymbol] = useState("");
  const [isPortfolioPickerOpen, setIsPortfolioPickerOpen] = useState(false);
  const [recentPortfolioIds, setRecentPortfolioIds] = useState<string[]>([]);

  const { data: similarStocks, isFetching: isSimilarFetching } = useSimilarStocks(similarSourceSymbol, 5);

  useEffect(() => {
    const stored = localStorage.getItem("portfolio:selected-id");
    if (stored) {
      setSelectedPortfolioId(stored);
    }

    setRecentPortfolioIds(readRecentPortfolioIds());
  }, []);

  const portfolios = data?.portfolios ?? [];

  useEffect(() => {
    if (portfolios.length === 0) {
      setSelectedPortfolioId(null);
      localStorage.removeItem("portfolio:selected-id");
      return;
    }

    if (!selectedPortfolioId || !portfolios.some((portfolio) => portfolio.id === selectedPortfolioId)) {
      const nextId = portfolios[0].id;
      setSelectedPortfolioId(nextId);
      localStorage.setItem("portfolio:selected-id", nextId);
    }
  }, [portfolios, selectedPortfolioId]);

  const selected = useMemo(
    () => portfolios.find((portfolio) => portfolio.id === selectedPortfolioId) ?? portfolios[0],
    [portfolios, selectedPortfolioId],
  );
  const recentPortfolios = useMemo(
    () => recentPortfolioIds
      .map((id) => portfolios.find((portfolio) => portfolio.id === id))
      .filter((portfolio): portfolio is (typeof portfolios)[number] => Boolean(portfolio)),
    [portfolios, recentPortfolioIds],
  );
  const nonRecentPortfolios = useMemo(
    () => portfolios.filter((portfolio) => !recentPortfolioIds.includes(portfolio.id)),
    [portfolios, recentPortfolioIds],
  );

  const suggestionCatalog = suggestionData?.catalog ?? [];
  const trendingGroups = suggestionData?.trending ?? [];
  const presets = suggestionData?.presets ?? [];

  useEffect(() => {
    if (!activeTrendingTab && trendingGroups.length > 0) {
      setActiveTrendingTab(trendingGroups[0].id);
    }
  }, [activeTrendingTab, trendingGroups]);

  useEffect(() => {
    if (!selected) {
      setSimilarSourceSymbol("");
      return;
    }

    if (!similarSourceSymbol || !selected.holdings.some((holding) => holding.symbol === similarSourceSymbol)) {
      setSimilarSourceSymbol(selected.holdings[0]?.symbol ?? "");
    }
  }, [selected, similarSourceSymbol]);

  const selectedSymbols = useMemo(() => {
    if (!selected) {
      return new Set<string>();
    }

    return new Set(selected.holdings.map((holding) => holding.symbol.toUpperCase()));
  }, [selected]);

  const sectors = useMemo(
    () => Array.from(new Set(suggestionCatalog.map((item) => item.sector))).sort((left, right) => left.localeCompare(right)),
    [suggestionCatalog],
  );

  const priceMin = parseOptionalNumber(catalogPriceMin);
  const priceMax = parseOptionalNumber(catalogPriceMax);
  const changeMin = parseOptionalNumber(catalogChangeMin);
  const changeMax = parseOptionalNumber(catalogChangeMax);

  const filteredCatalog = useMemo(() => {
    const query = catalogQuery.trim().toLowerCase();

    const filtered = suggestionCatalog.filter((stock) => {
      const matchesQuery = query.length === 0
        || stock.symbol.toLowerCase().includes(query)
        || stock.name.toLowerCase().includes(query);

      if (!matchesQuery) {
        return false;
      }

      if (catalogExchange !== "ALL" && stock.exchange !== catalogExchange) {
        return false;
      }

      if (catalogSector !== "ALL" && stock.sector !== catalogSector) {
        return false;
      }

      if (priceMin !== null && stock.price < priceMin) {
        return false;
      }

      if (priceMax !== null && stock.price > priceMax) {
        return false;
      }

      if (changeMin !== null && stock.weeklyChangePercent < changeMin) {
        return false;
      }

      if (changeMax !== null && stock.weeklyChangePercent > changeMax) {
        return false;
      }

      return true;
    });

    filtered.sort((left, right) => {
      switch (catalogSort) {
        case "symbol-desc":
          return right.symbol.localeCompare(left.symbol);
        case "price-desc":
          return right.price - left.price;
        case "price-asc":
          return left.price - right.price;
        case "change-desc":
          return right.weeklyChangePercent - left.weeklyChangePercent;
        case "change-asc":
          return left.weeklyChangePercent - right.weeklyChangePercent;
        case "volume-desc":
          return right.weeklyVolume - left.weeklyVolume;
        case "symbol-asc":
        default:
          return left.symbol.localeCompare(right.symbol);
      }
    });

    return filtered;
  }, [
    catalogChangeMax,
    catalogChangeMin,
    catalogExchange,
    catalogPriceMax,
    catalogPriceMin,
    catalogQuery,
    catalogSector,
    catalogSort,
    changeMax,
    changeMin,
    priceMax,
    priceMin,
    suggestionCatalog,
  ]);

  const visibleCatalog = useMemo(
    () => filteredCatalog.slice(0, visibleCatalogCount),
    [filteredCatalog, visibleCatalogCount],
  );

  const hasMoreCatalog = visibleCatalogCount < filteredCatalog.length;

  useEffect(() => {
    setVisibleCatalogCount(CATALOG_PAGE_SIZE);
  }, [catalogQuery, catalogExchange, catalogSector, catalogPriceMin, catalogPriceMax, catalogChangeMin, catalogChangeMax, catalogSort]);

  function notifyPortfolioAction(
    tone: "success" | "error" | "info",
    title: string,
    description: string,
  ) {
    createClientNotification({
      type: "portfolio_alert",
      title,
      message: description,
      payload: {
        source: "portfolio-tabs",
        tone,
      },
    });
    queryClient.invalidateQueries({ queryKey: ["notifications"] });
    showPortfolioMiniToast({ tone, title, description });
  }

  async function handleQuickAddStock(stock: Pick<StockCatalogItem, "symbol" | "price">, silent = false) {
    if (!selected) {
      return;
    }

    if (selectedSymbols.has(stock.symbol.toUpperCase())) {
      if (!silent) {
        notifyPortfolioAction("info", "Mã đã tồn tại", `${stock.symbol} đã có trong danh mục ${selected.name}.`);
      }
      return;
    }

    try {
      await addStockMutation.mutateAsync({
        portfolioId: selected.id,
        payload: {
          symbol: stock.symbol,
          shares: 100,
          purchasePrice: stock.price,
        },
        silent: true,
      });

      if (!silent) {
        notifyPortfolioAction("success", "Thêm cổ phiếu thành công", `Đã thêm ${stock.symbol} vào danh mục ${selected.name}.`);
      }
    } catch (error) {
      if (!silent) {
        const message = error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại";
        notifyPortfolioAction("error", "Không thể thêm cổ phiếu", message);
      }

      throw error;
    }
  }

  async function ensureQuickAddPortfolio() {
    if (selected) {
      return selected;
    }

    const created = await createPortfolioMutation.mutateAsync({
      name: "Danh mục mặc định",
      description: "Tạo tự động khi thêm nhanh từ mã gợi ý",
      totalInvestment: 100_000_000,
    });

    handleSelectPortfolio(created.id);
    return created;
  }

  async function handleQuickAddFromEmptyState(stock: Pick<StockCatalogItem, "symbol" | "price">) {
    try {
      const target = await ensureQuickAddPortfolio();

      await addStockMutation.mutateAsync({
        portfolioId: target.id,
        payload: {
          symbol: stock.symbol,
          shares: 100,
          purchasePrice: stock.price,
        },
        silent: true,
      });

      notifyPortfolioAction("success", "Thêm cổ phiếu thành công", `Đã thêm ${stock.symbol} vào danh mục ${target.name}.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại";
      notifyPortfolioAction("error", "Không thể thêm cổ phiếu", message);
    }
  }

  async function handleCopyPresetFromEmptyState(preset: SystemPortfolioPreset) {
    try {
      const target = await ensureQuickAddPortfolio();

      let successCount = 0;
      let failedCount = 0;

      for (const stock of preset.stocks) {
        try {
          await addStockMutation.mutateAsync({
            portfolioId: target.id,
            payload: {
              symbol: stock.symbol,
              shares: 100,
              purchasePrice: stock.price,
            },
            silent: true,
          });
          successCount += 1;
        } catch {
          failedCount += 1;
        }
      }

      if (successCount > 0) {
        notifyPortfolioAction(
          "success",
          "Sao chép danh mục thành công",
          `Đã thêm ${successCount} mã từ ${preset.name} vào ${target.name}.`,
        );
      }

      if (failedCount > 0) {
        notifyPortfolioAction("error", "Sao chép danh mục chưa hoàn tất", `Không thể thêm ${failedCount} mã từ ${preset.name}.`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Có lỗi xảy ra, vui lòng thử lại";
      notifyPortfolioAction("error", "Sao chép danh mục thất bại", message);
    }
  }

  async function handleCopyPreset(preset: SystemPortfolioPreset) {
    if (!selected) {
      return;
    }

    const pendingStocks = preset.stocks.filter((stock) => !selectedSymbols.has(stock.symbol.toUpperCase()));
    if (pendingStocks.length === 0) {
      notifyPortfolioAction("info", "Danh mục đã đủ mã", `${selected.name} đã có đầy đủ mã từ ${preset.name}.`);
      return;
    }

    let successCount = 0;
    let failedCount = 0;

    for (const stock of pendingStocks) {
      try {
        await handleQuickAddStock(stock, true);
        successCount += 1;
      } catch {
        failedCount += 1;
      }
    }

    if (successCount > 0) {
      notifyPortfolioAction("success", "Sao chép danh mục thành công", `Đã thêm ${successCount} mã từ ${preset.name} vào ${selected.name}.`);
    }

    if (failedCount > 0) {
      notifyPortfolioAction("error", "Sao chép danh mục chưa hoàn tất", `Không thể thêm ${failedCount} mã trong ${preset.name}.`);
    }
  }

  function handleSelectPortfolio(id: string) {
    setSelectedPortfolioId(id);
    localStorage.setItem("portfolio:selected-id", id);
    setRecentPortfolioIds(pushRecentPortfolioId(id));
  }

  function handleCreatedPortfolio(id: string) {
    handleSelectPortfolio(id);
  }

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

  if (!selected) {
    return (
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Danh mục đầu tư</h1>
            <p className="text-muted-foreground text-sm mt-1">Quản lý các danh mục cổ phiếu</p>
          </div>
          <CreatePortfolioDialog triggerLabel="Tạo danh mục" />
        </div>

        <Card className="border-dashed border-border">
          <CardContent className="py-10 flex flex-col items-center text-center gap-4">
            <div className="h-16 w-16 rounded-full bg-primary/10 text-primary flex items-center justify-center">
              <PieChartIcon className="h-8 w-8" />
            </div>
            <div className="space-y-1">
              <h2 className="text-xl font-semibold">Bạn chưa có danh mục nào</h2>
              <p className="text-sm text-muted-foreground">
                Tạo danh mục đầu tư để theo dõi hiệu suất cổ phiếu của bạn.
              </p>
            </div>
            <CreatePortfolioDialog triggerLabel="Tạo danh mục đầu tiên" />
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Mã cổ phiếu gợi ý</CardTitle>
          </CardHeader>
          <CardContent>
            {isSuggestionPending ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" /> Đang tải danh sách gợi ý...
              </div>
            ) : suggestionCatalog.length === 0 ? (
              <p className="text-sm text-muted-foreground">Chưa có dữ liệu mã gợi ý.</p>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
                {suggestionCatalog.slice(0, 12).map((stock) => (
                  <div key={stock.symbol} className="rounded-md border border-border p-3 space-y-2">
                    <div className="flex items-start justify-between gap-2">
                      <div>
                        <p className="font-semibold">{stock.symbol} - {stock.name}</p>
                        <p className="text-xs text-muted-foreground">{stock.exchange} • {stock.sector}</p>
                      </div>
                      <PercentBadge value={stock.weeklyChangePercent} showIcon={false} />
                    </div>
                    <div className="mt-2 flex items-center justify-between text-xs text-muted-foreground">
                      <span>Giá: {formatLocaleNumber(stock.price)}</span>
                      <span>KLGD: {formatLocaleNumber(stock.weeklyVolume)}</span>
                    </div>
                    <Button
                      size="sm"
                      className="w-full"
                      disabled={addStockMutation.isPending || createPortfolioMutation.isPending}
                      onClick={() => {
                        void handleQuickAddFromEmptyState(stock);
                      }}
                    >
                      <Plus className="mr-1 h-3.5 w-3.5" /> Thêm nhanh vào danh mục
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Danh mục hệ thống gợi ý</CardTitle>
          </CardHeader>
          <CardContent>
            {presets.length === 0 ? (
              <p className="text-sm text-muted-foreground">Chưa có dữ liệu danh mục gợi ý.</p>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
                {presets.map((preset) => (
                  <div key={preset.id} className="rounded-md border border-border p-4 space-y-3">
                    <div>
                      <h3 className="font-semibold">{preset.name}</h3>
                      <p className="mt-1 text-xs text-muted-foreground">{preset.criteria}</p>
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {preset.stocks.map((stock) => (
                        <span key={`${preset.id}-${stock.symbol}`} className="rounded bg-accent px-2 py-0.5 text-[11px]">
                          {stock.symbol}
                        </span>
                      ))}
                    </div>
                    <Button
                      size="sm"
                      className="w-full"
                      disabled={addStockMutation.isPending || createPortfolioMutation.isPending}
                      onClick={() => {
                        void handleCopyPresetFromEmptyState(preset);
                      }}
                    >
                      <Plus className="mr-1 h-3.5 w-3.5" /> Sao chép nhanh vào danh mục
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    );
  }

  const pieData = selected.holdings.map((h) => ({ name: h.symbol, value: h.weight }));

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Danh mục đầu tư</h1>
          <p className="text-muted-foreground text-sm mt-1">Quản lý các danh mục cổ phiếu</p>
        </div>
        <CreatePortfolioDialog triggerLabel="Tạo danh mục" onCreated={handleCreatedPortfolio} />
      </div>

      <Card className="bg-card border-border">
        <CardContent className="space-y-4 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="w-full max-w-lg">
              <Popover open={isPortfolioPickerOpen} onOpenChange={setIsPortfolioPickerOpen}>
                <PopoverTrigger asChild>
                  <Button
                    type="button"
                    variant="outline"
                    role="combobox"
                    aria-expanded={isPortfolioPickerOpen}
                    className="w-full justify-between"
                  >
                    <span className="truncate">{selected.name}</span>
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
                                handleSelectPortfolio(portfolio.id);
                                setIsPortfolioPickerOpen(false);
                              }}
                            >
                              <Check
                                className={cn(
                                  "mr-2 h-4 w-4",
                                  selected.id === portfolio.id ? "opacity-100" : "opacity-0",
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
                              handleSelectPortfolio(portfolio.id);
                              setIsPortfolioPickerOpen(false);
                            }}
                          >
                            <Check
                              className={cn(
                                "mr-2 h-4 w-4",
                                selected.id === portfolio.id ? "opacity-100" : "opacity-0",
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

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" className="h-8 w-8">
                  <MoreVertical className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem
                  onClick={() => {
                    setEditingPortfolio(selected);
                    setIsEditPortfolioOpen(true);
                  }}
                >
                  <Pencil className="mr-2 h-4 w-4" /> Sửa
                </DropdownMenuItem>
                <DropdownMenuItem
                  className="text-destructive focus:text-destructive"
                  onClick={() => {
                    setDeletingPortfolio(selected);
                    setIsDeletePortfolioOpen(true);
                  }}
                >
                  <Trash2 className="mr-2 h-4 w-4" /> Xóa
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>

          <div className="rounded-lg border border-border bg-background/30 p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h3 className="font-semibold">{selected.name}</h3>
                <p className="mt-1 text-2xl font-bold">{formatVND(selected.currentValue)}</p>
              </div>
              <PercentBadge value={selected.pnlPercent} />
            </div>

            <div className="mt-3 flex flex-wrap items-center gap-4 text-sm text-muted-foreground">
              <span>Đầu tư: {formatVND(selected.totalInvested)}</span>
              {selected.pnl !== 0 && (
                <span className={selected.pnl > 0 ? "text-stock-up" : "text-stock-down"}>
                  {selected.pnl > 0 ? "+" : ""}
                  {formatVND(selected.pnl)}
                </span>
              )}
              <span>{selected.holdings.length} mã</span>
            </div>

            {selected.description && <p className="mt-2 text-xs text-muted-foreground">{selected.description}</p>}

            <div className="mt-3 flex flex-wrap gap-1.5">
              {selected.holdings.map((holding) => (
                <span key={holding.symbol} className="rounded bg-accent px-2 py-0.5 text-xs">
                  {holding.symbol}
                </span>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2 bg-card border-border">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardTitle className="text-base">{selected.name} — Cổ phiếu</CardTitle>
            <div className="flex gap-2">
              <Button
                size="sm"
                variant="outline"
                className="gap-1 text-xs"
                onClick={() => navigate(`/toi-uu?portfolioId=${selected.id}`)}
              >
                <BarChart3 className="h-3 w-3" /> Tối ưu hóa
              </Button>
              <Button
                size="sm"
                variant="ghost"
                className="gap-1 text-xs"
                onClick={() => setIsAddStockOpen(true)}
              >
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
                    <th className="text-right py-2 font-medium">Giá mua</th>
                    <th className="text-right py-2 font-medium">Giá TT</th>
                    <th className="text-right py-2 font-medium">Lãi/Lỗ</th>
                    <th className="text-right py-2 font-medium">%</th>
                    <th className="text-right py-2 font-medium">Thao tác</th>
                  </tr>
                </thead>
                <tbody>
                  {selected.holdings.length > 0 ? (
                    selected.holdings.map((h) => {
                      const pnl = (h.currentPrice - h.avgPrice) * h.shares;
                      const pnlPct = ((h.currentPrice - h.avgPrice) / h.avgPrice) * 100;
                      return (
                        <tr key={h.id} className="border-b border-border/50 hover:bg-accent/50">
                          <td className="py-2.5 font-semibold">
                            <button
                              type="button"
                              className="hover:text-primary transition-colors"
                              onClick={() => navigate(`/co-phieu/${h.symbol}`)}
                            >
                              {h.symbol}
                            </button>
                          </td>
                          <td className="text-right tabular-nums">{h.avgPrice.toLocaleString("vi-VN")}</td>
                          <td className="text-right tabular-nums">{h.currentPrice.toLocaleString("vi-VN")}</td>
                          <td className={`text-right tabular-nums ${pnl > 0 ? "text-stock-up" : "text-stock-down"}`}>
                            {pnl > 0 ? "+" : ""}
                            {formatVND(pnl)}
                          </td>
                          <td className="text-right">
                            <PercentBadge value={pnlPct} showIcon={false} />
                          </td>
                          <td className="text-right">
                            <div className="inline-flex items-center gap-1">
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                onClick={() => {
                                  setEditingStock(h);
                                  setIsEditStockOpen(true);
                                }}
                              >
                                <Pencil className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8 text-destructive hover:text-destructive"
                                onClick={() => {
                                  setDeletingStock(h);
                                  setIsDeleteStockOpen(true);
                                }}
                              >
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            </div>
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

      <Card className="bg-card border-border">
        <CardHeader className="space-y-2 pb-2">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle className="text-base">Khám phá và gợi ý</CardTitle>
              <p className="mt-1 text-xs text-muted-foreground">
                Tập trung toàn bộ khu vực tìm mã, gợi ý xu hướng và danh mục hệ thống ở một nơi.
              </p>
            </div>
            <Button size="sm" variant="outline" className="gap-2" onClick={() => setIsAddStockOpen(true)}>
              <Plus className="h-4 w-4" /> Mở modal thêm nhanh
            </Button>
          </div>
        </CardHeader>

        <CardContent>
          <Tabs value={activePortfolioTab} onValueChange={setActivePortfolioTab} className="space-y-4">
            <TabsList className="h-auto flex-wrap bg-accent p-1">
              <TabsTrigger value="discover" className="text-xs">Khám phá mã</TabsTrigger>
              <TabsTrigger value="trending" className="text-xs">Gợi ý hôm nay</TabsTrigger>
              <TabsTrigger value="system" className="text-xs">Danh mục hệ thống</TabsTrigger>
            </TabsList>

            <TabsContent value="discover" className="space-y-4">
              <div className="rounded-md border border-border p-3">
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-3">
                  <div className="space-y-1">
                    <Label htmlFor="catalog-query">Tìm mã / tên</Label>
                    <Input
                      id="catalog-query"
                      placeholder="Ví dụ: VCB, Vinamilk"
                      value={catalogQuery}
                      onChange={(event) => setCatalogQuery(event.target.value)}
                    />
                  </div>

                  <div className="space-y-1">
                    <Label>Sàn</Label>
                    <Select value={catalogExchange} onValueChange={setCatalogExchange}>
                      <SelectTrigger>
                        <SelectValue placeholder="Tất cả sàn" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="ALL">Tất cả</SelectItem>
                        <SelectItem value="HOSE">HOSE</SelectItem>
                        <SelectItem value="HNX">HNX</SelectItem>
                        <SelectItem value="UPCOM">UPCOM</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-1">
                    <Label>Ngành</Label>
                    <Select value={catalogSector} onValueChange={setCatalogSector}>
                      <SelectTrigger>
                        <SelectValue placeholder="Tất cả ngành" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="ALL">Tất cả</SelectItem>
                        {sectors.map((sector) => (
                          <SelectItem key={sector} value={sector}>{sector}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-1">
                    <Label>Khoảng giá</Label>
                    <div className="flex items-center gap-2">
                      <Input placeholder="Từ" value={catalogPriceMin} onChange={(event) => setCatalogPriceMin(event.target.value)} />
                      <Input placeholder="Đến" value={catalogPriceMax} onChange={(event) => setCatalogPriceMax(event.target.value)} />
                    </div>
                  </div>

                  <div className="space-y-1">
                    <Label>Thay đổi 1 tuần (%)</Label>
                    <div className="flex items-center gap-2">
                      <Input placeholder="Từ" value={catalogChangeMin} onChange={(event) => setCatalogChangeMin(event.target.value)} />
                      <Input placeholder="Đến" value={catalogChangeMax} onChange={(event) => setCatalogChangeMax(event.target.value)} />
                    </div>
                  </div>
                </div>

                <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <Label className="text-xs text-muted-foreground">Sắp xếp:</Label>
                    <Select value={catalogSort} onValueChange={(value) => setCatalogSort(value as CatalogSortKey)}>
                      <SelectTrigger className="h-8 w-[220px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="symbol-asc">Mã A-Z</SelectItem>
                        <SelectItem value="symbol-desc">Mã Z-A</SelectItem>
                        <SelectItem value="price-desc">Giá cao-thấp</SelectItem>
                        <SelectItem value="price-asc">Giá thấp-cao</SelectItem>
                        <SelectItem value="change-desc">Tăng nhiều nhất</SelectItem>
                        <SelectItem value="change-asc">Giảm nhiều nhất</SelectItem>
                        <SelectItem value="volume-desc">Khối lượng giao dịch</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setCatalogQuery("");
                      setCatalogExchange("ALL");
                      setCatalogSector("ALL");
                      setCatalogPriceMin("");
                      setCatalogPriceMax("");
                      setCatalogChangeMin("");
                      setCatalogChangeMax("");
                      setCatalogSort("symbol-asc");
                    }}
                  >
                    Đặt lại bộ lọc
                  </Button>
                </div>
              </div>

              {isSuggestionPending ? (
                <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Đang tải danh sách cổ phiếu...
                </div>
              ) : filteredCatalog.length === 0 ? (
                <div className="rounded-md border border-dashed border-border p-6 text-center text-sm text-muted-foreground">
                  Không có mã nào phù hợp với bộ lọc hiện tại.
                </div>
              ) : (
                <>
                  <div className="overflow-x-auto rounded-md border border-border">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border text-xs text-muted-foreground">
                          <th className="px-3 py-2 text-left font-medium">Mã</th>
                          <th className="px-3 py-2 text-left font-medium">Công ty</th>
                          <th className="px-3 py-2 text-left font-medium">Sàn / Ngành</th>
                          <th className="px-3 py-2 text-right font-medium">Giá</th>
                          <th className="px-3 py-2 text-right font-medium">% 1 tuần</th>
                          <th className="px-3 py-2 text-right font-medium">KLGD 1 tuần</th>
                          <th className="px-3 py-2 text-right font-medium">Vốn hóa</th>
                          <th className="px-3 py-2 text-right font-medium">Thao tác</th>
                        </tr>
                      </thead>
                      <tbody>
                        {visibleCatalog.map((stock) => (
                          <tr key={stock.symbol} className="border-b border-border/60 hover:bg-accent/40">
                            <td className="px-3 py-2.5 font-semibold">{stock.symbol}</td>
                            <td className="px-3 py-2.5 text-muted-foreground">{stock.name}</td>
                            <td className="px-3 py-2.5 text-muted-foreground">{stock.exchange} • {stock.sector}</td>
                            <td className="px-3 py-2.5 text-right tabular-nums">{formatLocaleNumber(stock.price)}</td>
                            <td className={`px-3 py-2.5 text-right tabular-nums ${stock.weeklyChangePercent >= 0 ? "text-stock-up" : "text-stock-down"}`}>
                              {formatLocalePercent(stock.weeklyChangePercent)}
                            </td>
                            <td className="px-3 py-2.5 text-right tabular-nums">{formatLocaleNumber(stock.weeklyVolume)}</td>
                            <td className="px-3 py-2.5 text-right tabular-nums">{toCompactMarketCap(stock.marketCap)}</td>
                            <td className="px-3 py-2.5 text-right">
                              <Button
                                size="sm"
                                variant={selectedSymbols.has(stock.symbol) ? "outline" : "default"}
                                className="gap-1"
                                disabled={addStockMutation.isPending || selectedSymbols.has(stock.symbol)}
                                onClick={() => {
                                  void handleQuickAddStock(stock);
                                }}
                              >
                                <Plus className="h-3.5 w-3.5" />
                                {selectedSymbols.has(stock.symbol) ? "Đã có" : "Thêm"}
                              </Button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-xs text-muted-foreground">
                      Hiển thị {visibleCatalog.length}/{filteredCatalog.length} mã
                    </span>
                    {hasMoreCatalog && (
                      <Button
                        variant="outline"
                        size="sm"
                        className="gap-1"
                        onClick={() => setVisibleCatalogCount((current) => current + CATALOG_PAGE_SIZE)}
                      >
                        <ChevronDown className="h-3.5 w-3.5" /> Tải thêm 20 mã
                      </Button>
                    )}
                  </div>
                </>
              )}
            </TabsContent>

            <TabsContent value="trending" className="space-y-4">
              <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
                <div className="xl:col-span-2 rounded-md border border-border p-3">
                  <h3 className="text-sm font-semibold mb-3">Gợi ý hôm nay</h3>
                  {trendingGroups.length === 0 ? (
                    <div className="text-sm text-muted-foreground">Chưa có dữ liệu gợi ý xu hướng.</div>
                  ) : (
                    <Tabs value={activeTrendingTab} onValueChange={setActiveTrendingTab}>
                      <TabsList className="mb-3 h-auto flex-wrap bg-accent p-1">
                        {trendingGroups.map((group) => (
                          <TabsTrigger key={group.id} value={group.id} className="text-xs">
                            {group.title}
                          </TabsTrigger>
                        ))}
                      </TabsList>

                      {trendingGroups.map((group) => (
                        <TabsContent key={group.id} value={group.id} className="space-y-2">
                          <p className="text-xs text-muted-foreground">{group.description}</p>
                          {group.stocks.map((stock) => (
                            <div key={stock.symbol} className="flex items-center justify-between rounded-md border border-border px-3 py-2">
                              <div>
                                <p className="font-semibold">{stock.symbol} - {stock.name}</p>
                                <p className="text-xs text-muted-foreground">{stock.exchange} • {stock.sector}</p>
                              </div>
                              <div className="flex items-center gap-3">
                                <PercentBadge value={stock.weeklyChangePercent} showIcon={false} />
                                <Button
                                  size="sm"
                                  variant={selectedSymbols.has(stock.symbol) ? "outline" : "default"}
                                  disabled={selectedSymbols.has(stock.symbol) || addStockMutation.isPending}
                                  onClick={() => {
                                    void handleQuickAddStock(stock);
                                  }}
                                >
                                  {selectedSymbols.has(stock.symbol) ? "Đã có" : "Thêm"}
                                </Button>
                              </div>
                            </div>
                          ))}
                        </TabsContent>
                      ))}
                    </Tabs>
                  )}
                </div>

                <div className="rounded-md border border-border p-3">
                  <h3 className="text-sm font-semibold mb-3">Cổ phiếu tương tự</h3>
                  <div className="space-y-3">
                    {selected.holdings.length > 0 ? (
                      <Select value={similarSourceSymbol} onValueChange={setSimilarSourceSymbol}>
                        <SelectTrigger>
                          <SelectValue placeholder="Chọn mã nguồn" />
                        </SelectTrigger>
                        <SelectContent>
                          {selected.holdings.map((holding) => (
                            <SelectItem key={holding.id} value={holding.symbol}>{holding.symbol}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    ) : (
                      <p className="text-sm text-muted-foreground">Thêm mã vào danh mục để nhận gợi ý tương tự.</p>
                    )}

                    {isSimilarFetching ? (
                      <div className="flex items-center gap-2 text-sm text-muted-foreground">
                        <Loader2 className="h-4 w-4 animate-spin" /> Đang tải gợi ý tương tự...
                      </div>
                    ) : (
                      <div className="space-y-2">
                        {(similarStocks ?? []).length > 0 ? (
                          similarStocks?.map((stock) => (
                            <div key={stock.symbol} className="flex items-center justify-between rounded-md border border-border px-3 py-2">
                              <div>
                                <p className="font-semibold">{stock.symbol}</p>
                                <p className="text-xs text-muted-foreground">{stock.sector} • {stock.exchange}</p>
                              </div>
                              <Button
                                size="sm"
                                variant={selectedSymbols.has(stock.symbol) ? "outline" : "default"}
                                disabled={selectedSymbols.has(stock.symbol) || addStockMutation.isPending}
                                onClick={() => {
                                  void handleQuickAddStock(stock);
                                }}
                              >
                                {selectedSymbols.has(stock.symbol) ? "Đã có" : "Thêm"}
                              </Button>
                            </div>
                          ))
                        ) : (
                          <p className="text-sm text-muted-foreground">Chưa có dữ liệu gợi ý tương tự.</p>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="system">
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                {presets.map((preset) => (
                  <Card key={preset.id} className="border-border/80">
                    <CardContent className="p-4 space-y-3">
                      <div>
                        <h3 className="font-semibold">{preset.name}</h3>
                        <p className="mt-1 text-xs text-muted-foreground">{preset.description}</p>
                      </div>
                      <p className="text-xs text-muted-foreground">Tiêu chí: {preset.criteria}</p>
                      <div className="flex flex-wrap gap-1">
                        {preset.stocks.map((stock) => (
                          <span key={`${preset.id}-${stock.symbol}`} className="rounded bg-accent px-2 py-0.5 text-[11px]">
                            {stock.symbol}
                          </span>
                        ))}
                      </div>
                      <Button
                        size="sm"
                        className="w-full"
                        disabled={addStockMutation.isPending}
                        onClick={() => {
                          void handleCopyPreset(preset);
                        }}
                      >
                        Sao chép vào danh mục
                      </Button>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      <EditPortfolioDialog
        portfolio={editingPortfolio}
        open={isEditPortfolioOpen}
        onOpenChange={(open) => {
          setIsEditPortfolioOpen(open);
          if (!open) {
            setEditingPortfolio(null);
          }
        }}
      />
      <DeletePortfolioDialog
        portfolio={deletingPortfolio}
        open={isDeletePortfolioOpen}
        onOpenChange={(open) => {
          setIsDeletePortfolioOpen(open);
          if (!open) {
            setDeletingPortfolio(null);
          }
        }}
      />
      <AddStockDialog
        portfolioId={selected.id}
        open={isAddStockOpen}
        onOpenChange={setIsAddStockOpen}
      />
      <EditStockDialog
        portfolioId={selected.id}
        stock={editingStock}
        open={isEditStockOpen}
        onOpenChange={(open) => {
          setIsEditStockOpen(open);
          if (!open) {
            setEditingStock(null);
          }
        }}
      />
      <DeleteStockDialog
        portfolioId={selected.id}
        stock={deletingStock}
        open={isDeleteStockOpen}
        onOpenChange={(open) => {
          setIsDeleteStockOpen(open);
          if (!open) {
            setDeletingStock(null);
          }
        }}
      />
    </div>
  );
}
