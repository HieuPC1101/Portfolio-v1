import { useEffect, useId, useMemo, useRef, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Clock3, Loader2, Search, X } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import {
  clearStockSearchHistory,
  getStockSearchHistory,
  removeStockSearchHistory,
  saveStockSearchHistory,
  type StockSearchHistoryItem,
} from "@/repositories/stockSearchHistoryRepository";
import { searchStocks, type StockSearchResult } from "@/repositories/marketRepository";

interface StockAutocompleteProps {
  value: string;
  onValueChange: (value: string) => void;
  onSelect: (stock: StockSearchResult) => void;
  placeholder?: string;
  inputId?: string;
  limit?: number;
  historyKey?: string;
  className?: string;
  inputClassName?: string;
  disabled?: boolean;
  showHistory?: boolean;
  onQuickAction?: (stock: StockSearchResult) => void;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function renderHighlightedText(text: string, query: string): ReactNode {
  const normalizedQuery = query.trim();
  if (!normalizedQuery) {
    return text;
  }

  const matcher = new RegExp(`(${escapeRegExp(normalizedQuery)})`, "ig");
  const parts = text.split(matcher);

  return parts.map((part, index) => {
    const isMatched = part.localeCompare(normalizedQuery, undefined, { sensitivity: "accent" }) === 0;

    if (!isMatched) {
      return <span key={`${part}-${index}`}>{part}</span>;
    }

    return (
      <mark key={`${part}-${index}`} className="rounded-sm bg-primary/15 px-0.5 text-foreground">
        {part}
      </mark>
    );
  });
}

function formatPriceMeta(stock: StockSearchResult): string | null {
  if (typeof stock.price !== "number") {
    return null;
  }

  const priceLabel = stock.price.toLocaleString("vi-VN");
  if (typeof stock.percent !== "number") {
    return `${priceLabel} VND`;
  }

  const sign = stock.percent > 0 ? "+" : "";
  const percentLabel = new Intl.NumberFormat("vi-VN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(stock.percent);
  return `${priceLabel} VND (${sign}${percentLabel}%)`;
}

export function StockAutocomplete({
  value,
  onValueChange,
  onSelect,
  placeholder = "Nhập mã hoặc tên công ty...",
  inputId,
  limit = 10,
  historyKey,
  className,
  inputClassName,
  disabled,
  showHistory = true,
  onQuickAction,
}: StockAutocompleteProps) {
  const listboxId = useId();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [history, setHistory] = useState<StockSearchHistoryItem[]>(() => getStockSearchHistory(historyKey));

  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      setDebouncedQuery(value.trim());
    }, 300);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [value]);

  const { data, isFetching } = useQuery({
    queryKey: ["stock-autocomplete", debouncedQuery, limit],
    queryFn: () => searchStocks(debouncedQuery, limit),
    enabled: debouncedQuery.length > 0,
  });

  const isQueryEmpty = value.trim().length === 0;
  const shouldShowHistory = showHistory && isQueryEmpty;
  const results = useMemo(() => data ?? [], [data]);
  const items = shouldShowHistory ? history : results;

  useEffect(() => {
    if (activeIndex > items.length - 1) {
      setActiveIndex(items.length - 1);
    }
  }, [activeIndex, items.length]);

  useEffect(() => {
    function handleOutsideClick(event: MouseEvent) {
      if (!containerRef.current) {
        return;
      }

      if (!containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
        setActiveIndex(-1);
      }
    }

    document.addEventListener("mousedown", handleOutsideClick);
    return () => {
      document.removeEventListener("mousedown", handleOutsideClick);
    };
  }, []);

  const shouldShowDropdown = isOpen && (
    shouldShowHistory
    || value.trim().length > 0
    || isFetching
  );

  function refreshHistory() {
    if (!showHistory) {
      return;
    }

    setHistory(getStockSearchHistory(historyKey));
  }

  function handleSelectStock(stock: StockSearchResult) {
    onValueChange(stock.symbol);
    onSelect(stock);
    setHistory(saveStockSearchHistory(stock, historyKey));
    setIsOpen(false);
    setActiveIndex(-1);
  }

  function moveActiveIndex(direction: 1 | -1) {
    if (items.length === 0) {
      return;
    }

    setActiveIndex((current) => {
      if (current === -1) {
        return direction === 1 ? 0 : items.length - 1;
      }

      const next = current + direction;
      if (next < 0) {
        return items.length - 1;
      }

      if (next >= items.length) {
        return 0;
      }

      return next;
    });
  }

  return (
    <div ref={containerRef} className={cn("relative", className)}>
      <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
      <Input
        id={inputId}
        value={value}
        disabled={disabled}
        placeholder={placeholder}
        className={cn("pl-9 pr-9", inputClassName)}
        role="combobox"
        aria-expanded={shouldShowDropdown}
        aria-controls={listboxId}
        aria-activedescendant={activeIndex >= 0 ? `${listboxId}-${activeIndex}` : undefined}
        onFocus={() => {
          setIsOpen(true);
          refreshHistory();
        }}
        onChange={(event) => {
          onValueChange(event.target.value);
          setIsOpen(true);
          setActiveIndex(-1);
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            setIsOpen(false);
            setActiveIndex(-1);
            return;
          }

          if (event.key === "ArrowDown") {
            event.preventDefault();
            setIsOpen(true);
            moveActiveIndex(1);
            return;
          }

          if (event.key === "ArrowUp") {
            event.preventDefault();
            setIsOpen(true);
            moveActiveIndex(-1);
            return;
          }

          if (event.key === "Enter" && items.length > 0) {
            event.preventDefault();
            const selected = activeIndex >= 0 ? items[activeIndex] : items[0];
            handleSelectStock(selected);
          }
        }}
      />

      {isFetching && (
        <Loader2 className="absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-muted-foreground" />
      )}

      {shouldShowDropdown && (
        <div
          id={listboxId}
          role="listbox"
          className="absolute left-0 right-0 top-full z-50 mt-1 max-h-80 overflow-auto rounded-md border border-border bg-popover shadow-md"
        >
          {shouldShowHistory && (
            <div className="flex items-center justify-between border-b border-border px-3 py-2">
              <span className="text-xs font-medium text-muted-foreground">Tìm kiếm gần đây</span>
              {history.length > 0 && (
                <button
                  type="button"
                  className="text-xs text-muted-foreground hover:text-foreground"
                  onClick={() => {
                    clearStockSearchHistory(historyKey);
                    setHistory([]);
                  }}
                >
                  Xóa tất cả
                </button>
              )}
            </div>
          )}

          {isFetching ? (
            <div className="px-3 py-3 text-xs text-muted-foreground">Đang tải gợi ý...</div>
          ) : items.length === 0 ? (
            <div className="px-3 py-3 text-xs text-muted-foreground">
              {shouldShowHistory ? "Chưa có lịch sử tìm kiếm" : "Không tìm thấy mã phù hợp"}
            </div>
          ) : (
            <div className="py-1">
              {items.map((item, index) => {
                const displayName = item.name?.trim() ? item.name : "Không có tên công ty";
                const exchangeLabel = item.exchange?.trim() ? item.exchange : "N/A";
                const priceMeta = formatPriceMeta(item);

                return (
                  <div
                    key={`${item.symbol}-${index}`}
                    id={`${listboxId}-${index}`}
                    role="option"
                    aria-selected={activeIndex === index}
                    className={cn(
                      "group flex items-start justify-between gap-2 px-3 py-2 text-sm",
                      activeIndex === index ? "bg-accent" : "hover:bg-accent/70",
                    )}
                  >
                    <button
                      type="button"
                      className="flex min-w-0 flex-1 items-start gap-2 text-left"
                      onMouseEnter={() => setActiveIndex(index)}
                      onClick={() => handleSelectStock(item)}
                    >
                      {shouldShowHistory && <Clock3 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />}
                      <div className="min-w-0">
                        <p className="truncate font-semibold">
                          {renderHighlightedText(item.symbol, value)}
                          <span className="font-normal text-muted-foreground"> - {renderHighlightedText(displayName, value)}</span>
                        </p>
                        <p className="mt-0.5 text-xs text-muted-foreground">
                          {exchangeLabel}
                          {priceMeta ? ` • ${priceMeta}` : ""}
                        </p>
                      </div>
                    </button>

                    <div className="flex items-center gap-1">
                      {onQuickAction && (
                        <Button
                          type="button"
                          size="icon"
                          variant="ghost"
                          className="h-7 w-7"
                          onClick={() => onQuickAction(item)}
                        >
                          +
                        </Button>
                      )}

                      {shouldShowHistory && (
                        <button
                          type="button"
                          className="rounded p-1 text-muted-foreground opacity-0 transition-opacity hover:text-destructive group-hover:opacity-100"
                          onClick={(event) => {
                            event.stopPropagation();
                            const next = removeStockSearchHistory(item.symbol, historyKey);
                            setHistory(next);
                          }}
                        >
                          <X className="h-3.5 w-3.5" />
                        </button>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
