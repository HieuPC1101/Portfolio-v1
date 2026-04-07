import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";
import { useNewsQuery } from "@/hooks/useNewsQuery";
import { RefreshCw, ExternalLink, Clock } from "lucide-react";

export default function NewsPage() {
  const { data, refetch, isFetching, isPending, isError } = useNewsQuery();

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
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Tin tức thị trường</h1>
          <p className="text-muted-foreground text-sm mt-1">Cập nhật tin tức chứng khoán mới nhất</p>
        </div>
        <Button variant="outline" className="gap-2" disabled={isFetching} onClick={() => void refetch()}>
          <RefreshCw className={`h-4 w-4 ${isFetching ? "animate-spin" : ""}`} /> Tải lại
        </Button>
      </div>

      {data.items.length === 0 ? (
        <div className="text-sm text-muted-foreground">Chưa có tin tức mới.</div>
      ) : (
        <div className="space-y-3">
          {data.items.map((news) => (
            <Card key={news.id} className="bg-card border-border hover:border-primary/30 transition-colors cursor-pointer">
              <CardContent className="p-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="text-xs px-2 py-0.5 bg-primary/15 text-primary rounded-full font-medium">{news.category}</span>
                      <span className="text-xs text-muted-foreground flex items-center gap-1">
                        <Clock className="h-3 w-3" /> {news.time}
                      </span>
                    </div>
                    <h3 className="font-semibold text-sm leading-snug">{news.title}</h3>
                    <p className="text-xs text-muted-foreground mt-1 line-clamp-2">{news.summary}</p>
                    <p className="text-xs text-muted-foreground mt-2">Nguồn: {news.source}</p>
                  </div>
                  <Button asChild={Boolean(news.url)} variant="ghost" size="icon" className="shrink-0 text-muted-foreground h-8 w-8">
                    {news.url ? (
                      <a href={news.url} target="_blank" rel="noreferrer">
                        <ExternalLink className="h-4 w-4" />
                      </a>
                    ) : (
                      <span>
                        <ExternalLink className="h-4 w-4" />
                      </span>
                    )}
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
