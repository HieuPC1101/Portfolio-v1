import { useQuery } from "@tanstack/react-query";
import {
  getNotificationRules,
  getNotificationSettings,
  getNotifications,
  getUnreadNotificationCount,
} from "@/repositories/notificationRepository";
import type { NotificationStatus } from "@/types/notification";

export function useNotificationsFeedQuery(
  status: NotificationStatus | undefined = undefined,
) {
  return useQuery({
    queryKey: ["notifications", "feed", status ?? "all"],
    queryFn: () => getNotifications({ status, limit: 20, offset: 0 }),
    refetchInterval: 30_000,
  });
}

export function useUnreadNotificationsCountQuery() {
  return useQuery({
    queryKey: ["notifications", "unread-count"],
    queryFn: getUnreadNotificationCount,
    refetchInterval: 30_000,
  });
}

export function useNotificationRulesQuery() {
  return useQuery({
    queryKey: ["notifications", "rules"],
    queryFn: getNotificationRules,
  });
}

export function useNotificationSettingsQuery() {
  return useQuery({
    queryKey: ["notifications", "settings"],
    queryFn: getNotificationSettings,
  });
}
