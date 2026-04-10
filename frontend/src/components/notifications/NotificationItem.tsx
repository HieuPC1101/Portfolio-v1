import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { NotificationItemData } from "@/types/notification";
import { formatDistanceToNow } from "date-fns";
import { vi } from "date-fns/locale";

interface NotificationItemProps {
  notification: NotificationItemData;
  onMarkRead: (notificationId: number) => void;
  disabled?: boolean;
}

function getTypeLabel(type: NotificationItemData["type"]): string {
  if (type === "price_alert") {
    return "Giá";
  }
  if (type === "news_alert") {
    return "Tin tức";
  }
  if (type === "portfolio_alert") {
    return "Danh mục";
  }
  return "Hệ thống";
}

export function NotificationItem({
  notification,
  onMarkRead,
  disabled = false,
}: NotificationItemProps) {
  const createdAt = new Date(notification.created_at);
  const relativeTime = Number.isNaN(createdAt.getTime())
    ? "Vừa xong"
    : formatDistanceToNow(createdAt, { addSuffix: true, locale: vi });

  return (
    <article
      className={`rounded-lg border p-3 transition-colors ${
        notification.status === "unread" ? "bg-primary/5 border-primary/30" : "bg-background"
      }`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <p className="text-sm font-medium leading-none">{notification.title}</p>
            <Badge variant="outline" className="text-[10px] uppercase tracking-wide">
              {getTypeLabel(notification.type)}
            </Badge>
          </div>
          <p className="text-xs text-muted-foreground leading-relaxed">{notification.message}</p>
          <p className="text-[11px] text-muted-foreground">{relativeTime}</p>
        </div>

        {notification.status === "unread" ? (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            className="h-7 px-2 text-xs"
            disabled={disabled}
            onClick={() => onMarkRead(notification.id)}
          >
            Đã đọc
          </Button>
        ) : null}
      </div>
    </article>
  );
}
