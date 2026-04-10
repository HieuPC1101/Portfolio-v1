import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  useMarkAllNotificationsReadMutation,
  useMarkNotificationReadMutation,
} from "@/hooks/useNotificationMutations";
import {
  useNotificationsFeedQuery,
  useUnreadNotificationsCountQuery,
} from "@/hooks/useNotificationsQuery";
import { NotificationItem } from "@/components/notifications/NotificationItem";

export function NotificationPanel() {
  const { data, isLoading, error } = useNotificationsFeedQuery();
  const unreadCountQuery = useUnreadNotificationsCountQuery();
  const markReadMutation = useMarkNotificationReadMutation();
  const markAllMutation = useMarkAllNotificationsReadMutation();

  const notifications = data?.items ?? [];
  const unreadCount = unreadCountQuery.data?.unread_count ?? 0;
  const sortedNotifications = [...notifications].sort((left, right) => {
    const leftTime = new Date(left.created_at).getTime();
    const rightTime = new Date(right.created_at).getTime();

    if (Number.isNaN(leftTime) && Number.isNaN(rightTime)) {
      return 0;
    }

    if (Number.isNaN(leftTime)) {
      return 1;
    }

    if (Number.isNaN(rightTime)) {
      return -1;
    }

    return rightTime - leftTime;
  });
  const unreadNotifications = sortedNotifications.filter((item) => item.status === "unread");
  const readNotifications = sortedNotifications.filter((item) => item.status !== "unread");

  const hasItems = unreadNotifications.length > 0 || readNotifications.length > 0;
  const defaultTab = unreadNotifications.length > 0 ? "unread" : "read";

  function renderItems(items: typeof notifications) {
    if (items.length === 0) {
      return null;
    }

    return (
      <div className="space-y-2">
        {items.map((item) => (
          <NotificationItem
            key={item.id}
            notification={item}
            onMarkRead={(notificationId) =>
              markReadMutation.mutate(notificationId)
            }
            disabled={markReadMutation.isPending}
          />
        ))}
      </div>
    );
  }

  return (
    <div className="w-[360px] p-0">
      <div className="flex items-center justify-between p-4">
        <div>
          <p className="text-sm font-semibold">Thông báo</p>
          <p className="text-xs text-muted-foreground">
            {unreadCount > 0
              ? `${unreadCount} thông báo chưa đọc`
              : "Không có thông báo chưa đọc"}
          </p>
        </div>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          disabled={unreadCount === 0 || markAllMutation.isPending}
          onClick={() => markAllMutation.mutate()}
        >
          Đánh dấu tất cả
        </Button>
      </div>

      <Separator />
      {isLoading ? (
        <div className="p-3">
          <p className="py-6 text-center text-sm text-muted-foreground">
            Đang tải thông báo...
          </p>
        </div>
      ) : error ? (
        <div className="p-3">
          <p className="py-6 text-center text-sm text-destructive">
            Không thể tải thông báo. Vui lòng thử lại.
          </p>
        </div>
      ) : !hasItems ? (
        <div className="p-3">
          <p className="py-6 text-center text-sm text-muted-foreground">
            Bạn chưa có thông báo nào.
          </p>
        </div>
      ) : (
        <Tabs defaultValue={defaultTab} className="p-3 pt-2">
          <TabsList className="grid h-8 w-full grid-cols-2">
            <TabsTrigger value="unread" className="text-xs">
              Chưa đọc ({unreadNotifications.length})
            </TabsTrigger>
            <TabsTrigger value="read" className="text-xs">
              Đã đọc ({readNotifications.length})
            </TabsTrigger>
          </TabsList>

          <TabsContent value="unread" aria-label="Danh sách chưa đọc" className="mt-2">
            <ScrollArea className="h-[312px] pr-1">
              {unreadNotifications.length > 0 ? (
                renderItems(unreadNotifications)
              ) : (
                <p className="py-6 text-center text-sm text-muted-foreground">
                  Không có thông báo chưa đọc.
                </p>
              )}
            </ScrollArea>
          </TabsContent>

          <TabsContent value="read" aria-label="Danh sách đã đọc" className="mt-2">
            <ScrollArea className="h-[312px] pr-1">
              {readNotifications.length > 0 ? (
                renderItems(readNotifications)
              ) : (
                <p className="py-6 text-center text-sm text-muted-foreground">
                  Không có thông báo đã đọc.
                </p>
              )}
            </ScrollArea>
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
}
