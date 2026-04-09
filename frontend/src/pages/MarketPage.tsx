import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { MarketTabsLayout } from "@/components/market/MarketTabsLayout";
import { useMarketQuery } from "@/hooks/useMarketQuery";

export default function MarketPage() {
  const { data, isPending, isError } = useMarketQuery();

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
        <h1 className="text-2xl font-bold">Thị trường</h1>
        <p className="text-muted-foreground text-sm mt-1">Tổng quan thị trường chứng khoán Việt Nam</p>
      </div>

      <MarketTabsLayout data={data} />
    </div>
  );
}
