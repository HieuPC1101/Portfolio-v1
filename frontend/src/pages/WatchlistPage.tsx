import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { PercentBadge } from "@/components/common/PercentBadge";
import { StockAutocomplete } from "@/components/portfolio/StockAutocomplete";
import { showPortfolioMiniToast } from "@/components/notifications/PortfolioMiniToast";
import { usePortfolioSuggestions } from "@/hooks/usePortfolioSuggestions";
import { useAddWatchlistSymbol, useRemoveWatchlistSymbol } from "@/hooks/useWatchlistMutations";
import { useWatchlistQuery } from "@/hooks/useWatchlistQuery";
import type { StockSearchResult } from "@/repositories/marketRepository";
import { toast } from "sonner";
import { Eye, Loader2, Plus, X } from "lucide-react";

function formatLocaleNumber(value: number): string {
  return new Intl.NumberFormat("vi-VN").format(value);
}

export default function WatchlistPage() {
  const navigate = useNavigate();
  const { data, isPending, isError } = useWatchlistQuery();
  const { data: suggestionData, isPending: isSuggestionPending } = usePortfolioSuggestions();
  const addMutation = useAddWatchlistSymbol();
  const removeMutation = useRemoveWatchlistSymbol();
  const [activeTab, setActiveTab] = useState("search");
  const [searchValue, setSearchValue] = useState("");
  const [selectedStock, setSelectedStock] = useState<StockSearchResult | null>(null);
  const [removeSymbol, setRemoveSymbol] = useState<string | null>(null);

  const normalizedSearchSymbol = useMemo(() => searchValue.trim().toUpperCase(), [searchValue]);
  const selectedSymbol = selectedStock?.symbol.trim().toUpperCase() ?? "";
  const watchlistSymbols = useMemo(
    () => new Set((data?.items ?? []).map((item) => item.symbol.trim().toUpperCase())),
    [data?.items],
  );
  const suggestedStocks = useMemo(() => suggestionData?.catalog.slice(0, 12) ?? [], [suggestionData?.catalog]);
  const alreadyFollowingSelected = selectedSymbol.length > 0 && watchlistSymbols.has(selectedSymbol);
  const canFollowSelected = selectedSymbol.length > 0
    && normalizedSearchSymbol === selectedSymbol
    && !alreadyFollowingSelected
    && !addMutation.isPending;

  function handleFollowSymbol(symbol: string) {
    const normalizedSymbol = symbol.trim().toUpperCase();

    if (!normalizedSymbol) {
      toast.error("Vui lòng chọn mã hợp lệ từ gợi ý");
      return;
    }

    if (watchlistSymbols.has(normalizedSymbol)) {
      toast.error(`Mã ${normalizedSymbol} đã có trong danh sách theo dõi`);
      return;
    }

    addMutation.mutate(
      { symbol: normalizedSymbol },
      {
        onSuccess: () => {
          showPortfolioMiniToast({
            tone: "success",
            title: "Đã thêm vào theo dõi",
            description: `Đã thêm ${normalizedSymbol} vào danh sách theo dõi.`,
          });
          setActiveTab("watchlist");
        },
      },
    );
  }

  function handleFollowSelected() {
    handleFollowSymbol(selectedSymbol);
  }

  function handleSelectStock(stock: StockSearchResult) {
    setSelectedStock(stock);
    setSearchValue(stock.symbol);
  }

  function handleSearchValueChange(value: string) {
    setSearchValue(value);

    if (selectedStock && value.trim().toUpperCase() !== selectedStock.symbol.trim().toUpperCase()) {
      setSelectedStock(null);
    }
  }

  function navigateToStockDetail(symbol: string) {
    const normalizedSymbol = symbol.trim().toUpperCase();
    if (!normalizedSymbol) {
      return;
    }

    navigate(`/co-phieu/${encodeURIComponent(normalizedSymbol)}`);
  }

  function handleConfirmRemove() {
    if (!removeSymbol) {
      return;
    }

    removeMutation.mutate(
      { symbol: removeSymbol },
      {
        onSuccess: () => {
          setRemoveSymbol(null);
        },
      },
    );
  }

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
        <div>
          <h1 className="text-2xl font-bold">Cổ phiếu</h1>
          <p className="text-muted-foreground text-sm mt-1">Tìm kiếm, theo dõi và mở trang chi tiết mã cổ phiếu trong một nơi</p>
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
        <TabsList className="h-auto flex-wrap bg-accent p-1">
          <TabsTrigger value="search" className="text-xs">Tìm kiếm</TabsTrigger>
          <TabsTrigger value="watchlist" className="text-xs">Theo dõi</TabsTrigger>
        </TabsList>

        <TabsContent value="search" className="space-y-4 mt-0">
          <Card className="bg-card border-border">
            <CardContent className="p-4 md:p-5 space-y-4">
              <div>
                <p className="font-medium">Tìm mã cổ phiếu</p>
                <p className="text-xs text-muted-foreground mt-1">Nhập mã hoặc tên công ty để chọn nhanh, theo dõi ngay, hoặc xem chi tiết.</p>
              </div>

              <StockAutocomplete
                value={searchValue}
                onValueChange={handleSearchValueChange}
                onSelect={handleSelectStock}
                placeholder="Nhập mã hoặc tên công ty..."
                inputId="stocks-search-input"
                historyKey="stocks-module"
                disabled={addMutation.isPending}
                onQuickAction={(stock) => {
                  setSelectedStock(stock);
                  setSearchValue(stock.symbol);
                  handleFollowSymbol(stock.symbol);
                }}
              />

              <div className="rounded-md border border-border bg-muted/20 p-3 space-y-3">
                {selectedStock ? (
                  <>
                    <div className="flex flex-col gap-1">
                      <p className="font-semibold">{selectedStock.symbol}</p>
                      <p className="text-sm text-muted-foreground">{selectedStock.name ?? "Không có tên công ty"}</p>
                      <p className="text-xs text-muted-foreground">
                        {(selectedStock.exchange ?? "N/A")}
                        {selectedStock.sector ? ` • ${selectedStock.sector}` : ""}
                      </p>
                    </div>

                    {alreadyFollowingSelected && (
                      <p className="text-xs text-muted-foreground">Mã này đã có trong danh sách theo dõi.</p>
                    )}

                    <div className="flex flex-wrap items-center gap-2">
                      <Button
                        size="sm"
                        className="w-[132px] justify-center gap-2"
                        disabled={!canFollowSelected}
                        onClick={handleFollowSelected}
                      >
                        <Plus className="h-4 w-4" />
                        {addMutation.isPending ? "Đang thêm..." : "Theo dõi ngay"}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        className="w-[132px] justify-center gap-2"
                        onClick={() => navigateToStockDetail(selectedStock.symbol)}
                      >
                        <Eye className="h-4 w-4" /> Xem chi tiết
                      </Button>
                    </div>
                  </>
                ) : (
                  <p className="text-sm text-muted-foreground">Chọn một mã từ gợi ý để thực hiện thao tác nhanh.</p>
                )}
              </div>

              <div className="space-y-3 border-t border-border/70 pt-3">
                <div>
                  <p className="font-medium">Mã gợi ý cho bạn</p>
                  <p className="text-xs text-muted-foreground mt-1">Theo dõi nhanh các mã đang có xu hướng và thanh khoản tốt.</p>
                </div>

                {isSuggestionPending ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin" /> Đang tải mã gợi ý...
                  </div>
                ) : suggestedStocks.length === 0 ? (
                  <p className="text-sm text-muted-foreground">Chưa có dữ liệu mã gợi ý.</p>
                ) : (
                  <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
                    {suggestedStocks.map((stock) => {
                      const normalizedSymbol = stock.symbol.trim().toUpperCase();
                      const isFollowing = watchlistSymbols.has(normalizedSymbol);

                      return (
                        <div key={stock.symbol} className="rounded-md border border-border p-3 space-y-2">
                          <div className="flex items-start justify-between gap-2">
                            <div>
                              <p className="font-semibold">{stock.symbol}</p>
                              <p className="text-xs text-muted-foreground">{stock.name}</p>
                              <p className="text-xs text-muted-foreground mt-0.5">{stock.exchange} • {stock.sector}</p>
                            </div>
                            <PercentBadge value={stock.weeklyChangePercent} showIcon={false} />
                          </div>

                          <div className="flex items-center justify-between text-xs text-muted-foreground">
                            <span>Giá: {formatLocaleNumber(stock.price)}</span>
                            <span>KLGD: {formatLocaleNumber(stock.weeklyVolume)}</span>
                          </div>

                          <div className="flex flex-wrap items-center gap-2 pt-1">
                            <Button
                              size="sm"
                              className="w-[132px] justify-center gap-2"
                              variant={isFollowing ? "outline" : "default"}
                              disabled={isFollowing || addMutation.isPending}
                              onClick={() => handleFollowSymbol(stock.symbol)}
                            >
                              <Plus className="h-4 w-4" />
                              {isFollowing ? "Đã theo dõi" : "Theo dõi ngay"}
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              className="w-[132px] justify-center gap-2"
                              onClick={() => navigateToStockDetail(stock.symbol)}
                            >
                              <Eye className="h-4 w-4" /> Xem chi tiết
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="watchlist" className="space-y-4 mt-0">
          {data.items.length === 0 ? (
            <Card className="bg-card border-border">
              <CardContent className="p-6 flex items-center justify-between gap-4">
                <div>
                  <p className="font-medium">Danh sách theo dõi đang trống.</p>
                  <p className="text-sm text-muted-foreground mt-1">Thêm ít nhất một mã từ tab Tìm kiếm để bắt đầu theo dõi.</p>
                </div>
                <Button className="gap-2" onClick={() => setActiveTab("search")}>
                  <Plus className="h-4 w-4" /> Đi tới tìm kiếm
                </Button>
              </CardContent>
            </Card>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
              {data.items.map((stock) => (
                <Card
                  key={stock.symbol}
                  className="bg-card border-border hover:border-primary/50 transition-colors cursor-pointer group"
                  onClick={() => navigateToStockDetail(stock.symbol)}
                >
                  <CardContent className="p-4">
                    <div className="flex items-start justify-between">
                      <div>
                        <p className="font-bold text-lg">{stock.symbol}</p>
                        <p className="text-xs text-muted-foreground">{stock.name}</p>
                      </div>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity text-muted-foreground hover:text-destructive"
                        onClick={(event) => {
                          event.stopPropagation();
                          setRemoveSymbol(stock.symbol);
                        }}
                        aria-label={`Xoa ${stock.symbol} khoi danh sach theo doi`}
                        disabled={removeMutation.isPending}
                      >
                        <X className="h-3 w-3" />
                      </Button>
                    </div>
                    <p className="mt-3 text-xs text-muted-foreground">Nhấn để xem chi tiết mã cổ phiếu.</p>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </TabsContent>

      </Tabs>

      <AlertDialog
        open={Boolean(removeSymbol)}
        onOpenChange={(open) => {
          if (!open) {
            setRemoveSymbol(null);
          }
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Xóa mã khỏi danh sách theo dõi</AlertDialogTitle>
            <AlertDialogDescription>
              Bạn có chắc muốn xóa mã <strong>{removeSymbol ?? ""}</strong> khỏi danh sách theo dõi?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={removeMutation.isPending}>Hủy</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={(event) => {
                event.preventDefault();
                void handleConfirmRemove();
              }}
              disabled={removeMutation.isPending}
            >
              {removeMutation.isPending ? "Đang xóa..." : "Xóa mã"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
