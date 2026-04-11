import { useEffect, useRef } from "react";
import { AreaSeries, ColorType, createChart } from "lightweight-charts";
import type { StockPricePoint } from "@/repositories/marketRepository";
import { formatChartTick, formatChartTooltip } from "@/lib/chartTime";

interface AreaLineChartProps {
  data: StockPricePoint[];
}

export function AreaLineChart({ data }: AreaLineChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) {
      return;
    }

    const container = containerRef.current;

    const chart = createChart(container, {
      width: container.clientWidth,
      height: 320,
      layout: {
        background: { type: ColorType.Solid, color: "hsl(220, 22%, 12%)" },
        textColor: "hsl(215, 10%, 55%)",
      },
      grid: {
        vertLines: { color: "hsl(216, 14%, 18%)" },
        horzLines: { color: "hsl(216, 14%, 18%)" },
      },
      rightPriceScale: {
        borderColor: "hsl(216, 14%, 22%)",
      },
      timeScale: {
        borderColor: "hsl(216, 14%, 22%)",
        tickMarkFormatter: (time: unknown) => formatChartTick(time),
      },
      localization: {
        timeFormatter: (time: unknown) => formatChartTooltip(time),
      },
    });

    const series = chart.addSeries(AreaSeries, {
      lineColor: "hsl(142, 71%, 45%)",
      topColor: "hsla(142, 71%, 45%, 0.35)",
      bottomColor: "hsla(142, 71%, 45%, 0.02)",
      lineWidth: 2,
    });

    series.setData(
      data.map((point) => ({
        time: point.date,
        value: point.close * 1000,
      })),
    );

    chart.timeScale().fitContent();

    let resizeObserver: ResizeObserver | null = null;

    if (typeof ResizeObserver !== "undefined") {
      resizeObserver = new ResizeObserver(() => {
        chart.applyOptions({ width: container.clientWidth });
      });
      resizeObserver.observe(container);
    }

    return () => {
      resizeObserver?.disconnect();
      chart.remove();
    };
  }, [data]);

  return <div ref={containerRef} className="h-[320px] w-full" />;
}
