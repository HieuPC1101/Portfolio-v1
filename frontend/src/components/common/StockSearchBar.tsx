import { useEffect, useId, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { Loader2, Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { searchStocks, type StockSearchResult } from "@/repositories/marketRepository";

export function StockSearchBar() {
  const navigate = useNavigate();
  const listboxId = useId();
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);

  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      setDebouncedQuery(query.trim());
    }, 300);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [query]);

  const { data, isFetching } = useQuery({
    queryKey: ["stock-search", debouncedQuery],
    queryFn: () => searchStocks(debouncedQuery, 8),
    enabled: debouncedQuery.length >= 1,
  });

  const results = useMemo(() => data ?? [], [data]);
  const shouldShowDropdown = open && query.trim().length > 0;

  useEffect(() => {
    if (!debouncedQuery) {
      setOpen(false);
      setActiveIndex(-1);
      return;
    }

    setOpen(true);
  }, [debouncedQuery]);

  useEffect(() => {
    if (results.length === 0) {
      setActiveIndex(-1);
      return;
    }

    if (activeIndex > results.length - 1) {
      setActiveIndex(results.length - 1);
    }
  }, [activeIndex, results]);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (!containerRef.current) {
        return;
      }

      if (!containerRef.current.contains(event.target as Node)) {
        setOpen(false);
        setActiveIndex(-1);
      }
    }

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  function selectStock(stock: StockSearchResult) {
    setQuery(stock.symbol);
    setOpen(false);
    setActiveIndex(-1);
    navigate(`/co-phieu/${stock.symbol}`);
  }

  function moveActiveIndex(direction: 1 | -1) {
    if (results.length === 0) {
      return;
    }

    setActiveIndex((current) => {
      if (current === -1) {
        return direction === 1 ? 0 : results.length - 1;
      }

      const next = current + direction;
      if (next < 0) {
        return results.length - 1;
      }
      if (next >= results.length) {
        return 0;
      }

      return next;
    });
  }

  return (
    <div ref={containerRef} className="relative w-64">
      <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
      <Input
        value={query}
        placeholder="Tìm mã cổ phiếu..."
        className="h-9 w-full border-border bg-background pl-9 pr-9"
        role="combobox"
        aria-expanded={shouldShowDropdown}
        aria-controls={listboxId}
        aria-autocomplete="list"
        aria-activedescendant={activeIndex >= 0 ? `${listboxId}-${activeIndex}` : undefined}
        onFocus={() => {
          if (query.trim()) {
            setOpen(true);
          }
        }}
        onChange={(event) => {
          const nextQuery = event.target.value;
          setQuery(nextQuery);
          setOpen(nextQuery.trim().length > 0);
          setActiveIndex(-1);
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            setOpen(false);
            setActiveIndex(-1);
            return;
          }

          if (!query.trim()) {
            return;
          }

          if (event.key === "ArrowDown") {
            event.preventDefault();
            setOpen(true);
            moveActiveIndex(1);
            return;
          }

          if (event.key === "ArrowUp") {
            event.preventDefault();
            setOpen(true);
            moveActiveIndex(-1);
            return;
          }

          if (event.key === "Enter" && shouldShowDropdown && results.length > 0) {
            event.preventDefault();

            const selected = activeIndex >= 0 && activeIndex < results.length
              ? results[activeIndex]
              : results[0];

            selectStock(selected);
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
          className="absolute left-0 right-0 top-full z-50 mt-1 max-h-80 overflow-auto rounded-md border border-border bg-popover p-1 shadow-md"
        >
          {isFetching ? (
            <div className="px-2 py-2 text-xs text-muted-foreground">Đang tìm kiếm...</div>
          ) : results.length === 0 ? (
            <div className="px-2 py-2 text-xs text-muted-foreground">Không tìm thấy kết quả</div>
          ) : (
            results.map((item, index) => {
              const displayName = item.name?.trim() ? item.name : "Không có tên công ty";
              const exchangeLabel = item.exchange?.trim() ? item.exchange : "N/A";
              const optionLabel = `[${item.symbol}] - ${displayName} (${exchangeLabel})`;

              return (
                <button
                  key={`${item.symbol}-${index}`}
                  id={`${listboxId}-${index}`}
                  type="button"
                  role="option"
                  aria-selected={activeIndex === index}
                  className={`w-full rounded-sm px-2 py-2 text-left text-sm transition-colors ${activeIndex === index ? "bg-accent" : "hover:bg-accent/80"}`}
                  onMouseEnter={() => setActiveIndex(index)}
                  onClick={() => selectStock(item)}
                >
                  {optionLabel}
                </button>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}
