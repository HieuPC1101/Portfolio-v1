import { NotificationPanel } from "@/components/notifications/NotificationPanel";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { useUnreadNotificationsCountQuery } from "@/hooks/useNotificationsQuery";
import { Bell } from "lucide-react";

export function NotificationBell() {
  const { data } = useUnreadNotificationsCountQuery();
  const unreadCount = data?.unread_count ?? 0;

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="icon"
          className="relative text-muted-foreground"
          aria-label="Mở trung tâm thông báo"
        >
          <Bell className="h-4 w-4" />
          {unreadCount > 0 ? (
            <span className="absolute -right-0.5 -top-0.5 flex min-h-4 min-w-4 items-center justify-center rounded-full bg-destructive px-1 text-[10px] font-semibold text-destructive-foreground">
              {unreadCount > 99 ? "99+" : unreadCount}
            </span>
          ) : null}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-auto p-0" sideOffset={8}>
        <NotificationPanel />
      </PopoverContent>
    </Popover>
  );
}
