import type { MarketData } from "@/types/market";
import { MarketOverviewTab } from "@/components/market/tabs/MarketOverviewTab";

interface MarketTabsLayoutProps {
  data: MarketData;
}

export function MarketTabsLayout({ data }: MarketTabsLayoutProps) {
  return <MarketOverviewTab data={data} />;
}
