import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ResponsiveContainer, Treemap, Tooltip } from "recharts";
import type { MarketData } from "@/types/market";

interface SectorTreemapNode {
  name: string;
  size: number;
  change: number;
}

function formatWeight(value: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }

  return `${value.toLocaleString("vi-VN", { maximumFractionDigits: 1 })}%`;
}

function normalizeChange(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }

  return value;
}

function formatChange(value: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }

  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toLocaleString("vi-VN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
}

function sanitizeSectorName(name: string): string {
  const trimmedName = name?.trim() ?? "";

  if (!trimmedName) {
    return "Không xác định";
  }

  return trimmedName;
}

function isInvalidSectorName(name: string): boolean {
  const normalizedName = name?.trim().toLowerCase() ?? "";

  return !normalizedName || normalizedName === "nan" || normalizedName === "n/a" || normalizedName === "na";
}

function getSectorFillColor(change: number): string {
  const safeChange = normalizeChange(change);
  const absChange = Math.min(Math.abs(safeChange), 8);
  const intensity = absChange / 7;

  if (Math.abs(safeChange) < 0.15) {
    return "hsl(216, 21%, 20%)";
  }

  if (safeChange > 0) {
    return `hsl(145, ${52 + intensity * 10}%, ${36 - intensity * 8}%)`;
  }

  return `hsl(2, ${68 + intensity * 8}%, ${38 - intensity * 8}%)`;
}

function SectorTreemapCell(props: Record<string, unknown>) {
  const x = Number(props.x ?? 0);
  const y = Number(props.y ?? 0);
  const width = Number(props.width ?? 0);
  const height = Number(props.height ?? 0);
  const payload =
    (props.payload as SectorTreemapNode | undefined) ??
    (typeof props.name === "string"
      ? {
          name: props.name,
          size: Number(props.size ?? props.value ?? 0),
          change: Number(props.change ?? 0),
        }
      : undefined);

  if (!payload || width <= 0 || height <= 0) {
    return null;
  }

  const showName = width > 84 && height > 24;
  const showChange = width > 88 && height > 42;
  const showWeight = width > 96 && height > 58;
  const baseTextStyle = {
    fontFamily: "'Be Vietnam Pro', sans-serif",
    paintOrder: "stroke",
    stroke: "hsl(216, 20%, 12%)",
    strokeWidth: 1.4,
    strokeLinejoin: "round" as const,
  };

  return (
    <g>
      <rect
        x={x + 1}
        y={y + 1}
        width={Math.max(0, width - 2)}
        height={Math.max(0, height - 2)}
        rx={8}
        ry={8}
        fill={getSectorFillColor(payload.change)}
        stroke="hsl(216, 20%, 16%)"
        strokeWidth={1}
      />

      {showName ? (
        <text
          x={x + 10}
          y={y + 20}
          fill="hsl(213, 27%, 92%)"
          fontSize={13}
          fontWeight={600}
          style={baseTextStyle}
        >
          {sanitizeSectorName(payload.name)}
        </text>
      ) : null}

      {showChange ? (
        <text
          x={x + 10}
          y={y + 38}
          fill="hsl(213, 27%, 92%)"
          fontSize={12}
          fontWeight={600}
          className="tabular-nums"
          style={baseTextStyle}
        >
          {formatChange(payload.change)}
        </text>
      ) : null}

      {showWeight ? (
        <text
          x={x + 10}
          y={y + 56}
          fill="hsl(215, 12%, 70%)"
          fontSize={11}
          fontWeight={500}
          style={baseTextStyle}
        >
          Tỷ trọng {formatWeight(payload.size)}
        </text>
      ) : null}
    </g>
  );
}

function SectorTooltipContent(props: { active?: boolean; payload?: Array<{ payload?: SectorTreemapNode }> }) {
  const active = props.active ?? false;
  const payload = props.payload?.[0]?.payload;

  if (!active || !payload) {
    return null;
  }

  return (
    <div className="min-w-[140px] rounded-md border border-border/80 bg-card/95 px-2.5 py-2 shadow-lg backdrop-blur-sm">
      <p className="text-xs font-semibold text-foreground">{sanitizeSectorName(payload.name)}</p>
      <p className={`mt-1 text-xs font-semibold tabular-nums ${payload.change >= 0 ? "text-stock-up" : "text-stock-down"}`}>
        {formatChange(payload.change)}
      </p>
      <p className="mt-0.5 text-xs text-muted-foreground">Tỷ trọng {formatWeight(payload.size)}</p>
    </div>
  );
}

interface MarketSectorsTabProps {
  data: MarketData;
}

export function MarketSectorsTab({ data }: MarketSectorsTabProps) {
  const visibleSectors = data.sectors.filter((sector) => !isInvalidSectorName(sector.name));

  if (visibleSectors.length === 0) {
    return (
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Bản đồ ngành</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-sm text-muted-foreground">Chưa có dữ liệu ngành hợp lệ.</div>
        </CardContent>
      </Card>
    );
  }

  const sectors = [...visibleSectors].sort((left, right) => right.size - left.size);
  const topGainer = [...visibleSectors].sort((left, right) => right.change - left.change)[0];
  const topLoser = [...visibleSectors].sort((left, right) => left.change - right.change)[0];
  const largestSector = sectors[0];
  const totalWeight = sectors.reduce((sum, sector) => sum + Math.max(0, sector.size), 0);
  const normalizedTotalWeight = totalWeight > 0 ? totalWeight : 1;

  const treemapData: SectorTreemapNode[] = sectors.map((sector) => ({
    name: sanitizeSectorName(sector.name),
    size: Math.max(0, (Math.max(0, sector.size) / normalizedTotalWeight) * 100),
    change: normalizeChange(sector.change),
  }));

  const riseCount = sectors.filter((sector) => sector.change > 0).length;
  const fallCount = sectors.filter((sector) => sector.change < 0).length;

  return (
    <Card className="border-border bg-card">
      <CardHeader className="pb-2">
        <CardTitle className="text-base">Bản đồ ngành</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-lg border border-border/70 bg-accent/30 p-3 text-sm">
            <p className="text-xs text-muted-foreground">Số ngành tăng / giảm</p>
            <p className="mt-1 font-semibold tabular-nums">
              <span className="text-stock-up">{riseCount}</span>
              <span className="mx-1 text-muted-foreground">/</span>
              <span className="text-stock-down">{fallCount}</span>
            </p>
          </div>
          <div className="rounded-lg border border-border/70 bg-accent/30 p-3 text-sm">
            <p className="text-xs text-muted-foreground">Ngành tăng mạnh nhất</p>
            <p className="mt-1 line-clamp-1 font-semibold text-stock-up">{topGainer ? sanitizeSectorName(topGainer.name) : "--"}</p>
            <p className="tabular-nums text-stock-up">{topGainer ? formatChange(topGainer.change) : "--"}</p>
          </div>
          <div className="rounded-lg border border-border/70 bg-accent/30 p-3 text-sm">
            <p className="text-xs text-muted-foreground">Ngành giảm mạnh nhất</p>
            <p className="mt-1 line-clamp-1 font-semibold text-stock-down">{topLoser ? sanitizeSectorName(topLoser.name) : "--"}</p>
            <p className="tabular-nums text-stock-down">{topLoser ? formatChange(topLoser.change) : "--"}</p>
          </div>
          <div className="rounded-lg border border-border/70 bg-accent/30 p-3 text-sm">
            <p className="text-xs text-muted-foreground">Ngành chiếm tỷ trọng lớn nhất</p>
            <p className="mt-1 line-clamp-1 font-semibold text-foreground">{largestSector ? sanitizeSectorName(largestSector.name) : "--"}</p>
            <p className="tabular-nums text-muted-foreground">{largestSector ? formatWeight((Math.max(0, largestSector.size) / normalizedTotalWeight) * 100) : "--"}</p>
          </div>
        </div>

        <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border/70 bg-accent/20 px-3 py-2 text-xs text-muted-foreground">
          <div className="flex flex-wrap items-center gap-3">
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-sm bg-stock-up" />
              Tăng
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-sm bg-[hsl(2,68%,38%)]" />
              Giảm nhẹ
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-sm bg-[hsl(2,74%,32%)]" />
              Giảm mạnh
            </span>
          </div>
          <span>Kích thước ô = tỷ trọng vốn hóa ngành</span>
        </div>

        <div className="h-[280px] overflow-hidden rounded-lg border border-border/70 bg-accent/20 p-2 sm:h-[320px]">
          <ResponsiveContainer width="100%" height="100%">
            <Treemap data={treemapData} dataKey="size" stroke="hsl(216, 20%, 14%)" isAnimationActive={false} content={<SectorTreemapCell />}>
              <Tooltip
                cursor={{ stroke: "hsl(213, 27%, 92%)", strokeOpacity: 0.22, strokeWidth: 1 }}
                content={<SectorTooltipContent />}
              />
            </Treemap>
          </ResponsiveContainer>
        </div>

        <div className="rounded-lg border border-border/70 bg-accent/30 p-3">
          <h3 className="text-sm font-semibold">Biến động theo ngành</h3>
          <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
            {sectors.map((sector) => (
              <div key={`${sector.name}-detail`} className="flex items-center justify-between rounded border border-border/60 bg-card/40 px-2.5 py-2 text-sm">
                <span className="line-clamp-1">{sanitizeSectorName(sector.name)}</span>
                <span className={sector.change >= 0 ? "tabular-nums text-stock-up" : "tabular-nums text-stock-down"}>
                  {formatChange(sector.change)}
                </span>
              </div>
            ))}
          </div>
        </div>

      </CardContent>
    </Card>
  );
}
