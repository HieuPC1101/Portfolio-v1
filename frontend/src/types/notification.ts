export type NotificationType =
  | "price_alert"
  | "news_alert"
  | "portfolio_alert"
  | "system_alert";

export type NotificationStatus = "unread" | "read" | "archived";

export type NotificationRuleType =
  | "price_cross"
  | "price_change_percent"
  | "news_watchlist";

export type NotificationOperator =
  | "gt"
  | "lt"
  | "gte"
  | "lte"
  | "cross_up"
  | "cross_down";

export interface NotificationItemData {
  id: number;
  user_id: number;
  type: NotificationType;
  title: string;
  message: string;
  payload?: Record<string, unknown> | null;
  status: NotificationStatus;
  created_at: string;
  read_at?: string | null;
}

export interface NotificationListResponse {
  items: NotificationItemData[];
  total: number;
  limit: number;
  offset: number;
}

export interface NotificationUnreadCountResponse {
  unread_count: number;
}

export interface MarkAllNotificationsReadResponse {
  updated: number;
}

export interface NotificationRule {
  id: number;
  user_id: number;
  rule_type: NotificationRuleType;
  symbol?: string | null;
  operator?: NotificationOperator | null;
  threshold_value?: number | null;
  is_active: boolean;
  cooldown_minutes: number;
  last_triggered_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface NotificationRuleCreatePayload {
  rule_type: NotificationRuleType;
  symbol?: string;
  operator?: NotificationOperator;
  threshold_value?: number;
  is_active?: boolean;
  cooldown_minutes?: number;
}

export interface NotificationRuleUpdatePayload {
  rule_type?: NotificationRuleType;
  symbol?: string | null;
  operator?: NotificationOperator | null;
  threshold_value?: number | null;
  is_active?: boolean;
  cooldown_minutes?: number;
}

export interface NotificationSettings {
  notifications_enabled: boolean;
  notification_price_enabled: boolean;
  notification_news_enabled: boolean;
  notification_portfolio_enabled: boolean;
}

export interface NotificationSettingsUpdatePayload {
  notifications_enabled?: boolean;
  notification_price_enabled?: boolean;
  notification_news_enabled?: boolean;
  notification_portfolio_enabled?: boolean;
}
