import { useEffect, useRef } from "react";
import { CandlestickSeries, ColorType, createChart } from "lightweight-charts";
import type { OHLCPoint } from "@/repositories/marketRepository";
import { formatChartTick, formatChartTooltip } from "@/lib/chartTime";

interface CandlestickChartProps {
  data: OHLCPoint[];
}

export function CandlestickChart({ data }: CandlestickChartProps) {
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

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
      borderVisible: false,
    });

    series.setData(
      data.map((point) => ({
        time: point.date,
        open: point.open * 1000,
        high: point.high * 1000,
        low: point.low * 1000,
        close: point.close * 1000,
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
