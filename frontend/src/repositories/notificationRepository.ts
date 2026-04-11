import { ENABLE_MOCK_API, MOCK_API_DELAY_MS } from "@/config/runtime";
import {
  apiAuthDelete,
  apiAuthGet,
  apiAuthPatch,
  apiAuthPost,
  apiAuthPut,
} from "@/lib/apiAuth";
import type {
  MarkAllNotificationsReadResponse,
  NotificationItemData,
  NotificationListResponse,
  NotificationRule,
  NotificationRuleCreatePayload,
  NotificationRuleUpdatePayload,
  NotificationSettings,
  NotificationSettingsUpdatePayload,
  NotificationStatus,
  NotificationUnreadCountResponse,
} from "@/types/notification";

interface GetNotificationsParams {
  status?: NotificationStatus;
  limit?: number;
  offset?: number;
}

interface CreateClientNotificationPayload {
  type: NotificationItemData["type"];
  title: string;
  message: string;
  payload?: Record<string, unknown> | null;
}

const DEFAULT_LIMIT = 20;
const MAX_LOCAL_NOTIFICATIONS = 100;

let localNotificationSequence = -1;
let localNotifications: NotificationItemData[] = [];

let mockNotifications: NotificationItemData[] = [
  {
    id: 1,
    user_id: 1,
    type: "price_alert",
    title: "FPT vượt ngưỡng 140000",
    message: "Giá hiện tại 140500 (+2.1%)",
    payload: {
      symbol: "FPT",
      current_price: 140500,
      threshold: 140000,
      direction: "cross_up",
    },
    status: "unread",
    created_at: new Date(Date.now() - 1000 * 60 * 15).toISOString(),
    read_at: null,
  },
  {
    id: 2,
    user_id: 1,
    type: "news_alert",
    title: "Có tin mới liên quan VNM",
    message: "VNM công bố kết quả kinh doanh quý mới.",
    payload: {
      symbol: "VNM",
      source: "VnExpress",
    },
    status: "unread",
    created_at: new Date(Date.now() - 1000 * 60 * 45).toISOString(),
    read_at: null,
  },
  {
    id: 3,
    user_id: 1,
    type: "system_alert",
    title: "Bảo mật tài khoản",
    message: "Mật khẩu của bạn đã được cập nhật thành công.",
    payload: null,
    status: "read",
    created_at: new Date(Date.now() - 1000 * 60 * 120).toISOString(),
    read_at: new Date(Date.now() - 1000 * 60 * 110).toISOString(),
  },
];

let mockRules: NotificationRule[] = [
  {
    id: 1,
    user_id: 1,
    rule_type: "price_cross",
    symbol: "FPT",
    operator: "cross_up",
    threshold_value: 140000,
    is_active: true,
    cooldown_minutes: 60,
    last_triggered_at: null,
    created_at: new Date(Date.now() - 1000 * 60 * 60 * 24).toISOString(),
    updated_at: new Date(Date.now() - 1000 * 60 * 60 * 24).toISOString(),
  },
];

let mockSettings: NotificationSettings = {
  notifications_enabled: true,
  notification_price_enabled: true,
  notification_news_enabled: true,
  notification_portfolio_enabled: true,
};

function sortNotificationsByTime(items: NotificationItemData[]): NotificationItemData[] {
  return [...items].sort((left, right) => {
    const leftTime = new Date(left.created_at).getTime();
    const rightTime = new Date(right.created_at).getTime();

    if (Number.isNaN(leftTime) && Number.isNaN(rightTime)) {
      return right.id - left.id;
    }

    if (Number.isNaN(leftTime)) {
      return 1;
    }

    if (Number.isNaN(rightTime)) {
      return -1;
    }

    if (leftTime === rightTime) {
      return right.id - left.id;
    }

    return rightTime - leftTime;
  });
}

function mergeNotifications(baseItems: NotificationItemData[]): NotificationItemData[] {
  const mergedMap = new Map<number, NotificationItemData>();

  for (const item of baseItems) {
    mergedMap.set(item.id, item);
  }

  for (const item of localNotifications) {
    mergedMap.set(item.id, item);
  }

  return sortNotificationsByTime(Array.from(mergedMap.values()));
}

function filterNotificationsByStatus(
  items: NotificationItemData[],
  status?: NotificationStatus,
): NotificationItemData[] {
  if (!status) {
    return items;
  }

  return items.filter((item) => item.status === status);
}

export function createClientNotification(
  payload: CreateClientNotificationPayload,
): NotificationItemData {
  const created: NotificationItemData = {
    id: localNotificationSequence,
    user_id: 0,
    type: payload.type,
    title: payload.title,
    message: payload.message,
    payload: payload.payload ?? null,
    status: "unread",
    created_at: new Date().toISOString(),
    read_at: null,
  };

  localNotificationSequence -= 1;
  localNotifications = [created, ...localNotifications].slice(0, MAX_LOCAL_NOTIFICATIONS);

  return created;
}

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function buildNotificationQuery(params: GetNotificationsParams): string {
  const searchParams = new URLSearchParams();
  if (params.status) {
    searchParams.set("status", params.status);
  }
  searchParams.set("limit", String(params.limit ?? DEFAULT_LIMIT));
  searchParams.set("offset", String(params.offset ?? 0));
  return searchParams.toString();
}

async function getMockNotifications(
  params: GetNotificationsParams,
): Promise<NotificationListResponse> {
  await delay(MOCK_API_DELAY_MS);
  const status = params.status;
  const limit = params.limit ?? DEFAULT_LIMIT;
  const offset = params.offset ?? 0;

  const filtered = filterNotificationsByStatus(
    mergeNotifications(mockNotifications),
    status,
  );
  const items = filtered.slice(offset, offset + limit);

  return {
    items,
    total: filtered.length,
    limit,
    offset,
  };
}

async function getLiveNotifications(
  params: GetNotificationsParams,
): Promise<NotificationListResponse> {
  const query = buildNotificationQuery(params);
  const response = await apiAuthGet<NotificationListResponse>(`/api/v1/notifications?${query}`);
  const status = params.status;
  const limit = params.limit ?? DEFAULT_LIMIT;
  const offset = params.offset ?? 0;

  const filtered = filterNotificationsByStatus(mergeNotifications(response.items), status);
  const localTotal = filterNotificationsByStatus(localNotifications, status).length;

  return {
    items: filtered.slice(offset, offset + limit),
    total: response.total + localTotal,
    limit,
    offset,
  };
}

export async function getNotifications(
  params: GetNotificationsParams = {},
): Promise<NotificationListResponse> {
  if (ENABLE_MOCK_API) {
    return getMockNotifications(params);
  }

  return getLiveNotifications(params);
}

export async function getUnreadNotificationCount(): Promise<NotificationUnreadCountResponse> {
  const localUnread = localNotifications.filter((item) => item.status === "unread")
    .length;

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return {
      unread_count: mockNotifications.filter((item) => item.status === "unread")
        .length + localUnread,
    };
  }

  const response = await apiAuthGet<NotificationUnreadCountResponse>(
    "/api/v1/notifications/unread-count",
  );

  return {
    unread_count: response.unread_count + localUnread,
  };
}

export async function markNotificationAsRead(
  notificationId: number,
): Promise<NotificationItemData> {
  const localTarget = localNotifications.find((item) => item.id === notificationId);
  if (localTarget) {
    if (localTarget.status === "unread") {
      localTarget.status = "read";
      localTarget.read_at = localTarget.read_at ?? new Date().toISOString();
    }

    return localTarget;
  }

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);

    const target = mockNotifications.find((item) => item.id === notificationId);
    if (!target) {
      throw new Error("Không tìm thấy thông báo");
    }

    target.status = "read";
    target.read_at = target.read_at ?? new Date().toISOString();
    return target;
  }

  return apiAuthPatch<NotificationItemData>(
    `/api/v1/notifications/${notificationId}/read`,
  );
}

export async function markAllNotificationsAsRead(): Promise<MarkAllNotificationsReadResponse> {
  let localUpdated = 0;
  localNotifications = localNotifications.map((item) => {
    if (item.status === "unread") {
      localUpdated += 1;
      return {
        ...item,
        status: "read",
        read_at: item.read_at ?? new Date().toISOString(),
      };
    }

    return item;
  });

  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    let updated = 0;

    mockNotifications = mockNotifications.map((item) => {
      if (item.status === "unread") {
        updated += 1;
        return {
          ...item,
          status: "read",
          read_at: new Date().toISOString(),
        };
      }
      return item;
    });

    return { updated: updated + localUpdated };
  }

  const response = await apiAuthPatch<MarkAllNotificationsReadResponse>(
    "/api/v1/notifications/read-all",
  );

  return {
    updated: response.updated + localUpdated,
  };
}

export async function getNotificationRules(): Promise<NotificationRule[]> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return mockRules;
  }

  return apiAuthGet<NotificationRule[]>("/api/v1/notifications/rules");
}

export async function createNotificationRule(
  payload: NotificationRuleCreatePayload,
): Promise<NotificationRule> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const id = mockRules.length
      ? Math.max(...mockRules.map((rule) => rule.id)) + 1
      : 1;
    const now = new Date().toISOString();
    const rule: NotificationRule = {
      id,
      user_id: 1,
      rule_type: payload.rule_type,
      symbol: payload.symbol ?? null,
      operator: payload.operator ?? null,
      threshold_value: payload.threshold_value ?? null,
      is_active: payload.is_active ?? true,
      cooldown_minutes: payload.cooldown_minutes ?? 60,
      last_triggered_at: null,
      created_at: now,
      updated_at: now,
    };
    mockRules = [rule, ...mockRules];
    return rule;
  }

  return apiAuthPost<NotificationRule>("/api/v1/notifications/rules", payload);
}

export async function updateNotificationRule(
  ruleId: number,
  payload: NotificationRuleUpdatePayload,
): Promise<NotificationRule> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const target = mockRules.find((rule) => rule.id === ruleId);
    if (!target) {
      throw new Error("Không tìm thấy rule thông báo");
    }
    Object.assign(target, payload, { updated_at: new Date().toISOString() });
    return target;
  }

  return apiAuthPut<NotificationRule>(`/api/v1/notifications/rules/${ruleId}`, payload);
}

export async function toggleNotificationRule(
  ruleId: number,
  isActive: boolean,
): Promise<NotificationRule> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    const target = mockRules.find((rule) => rule.id === ruleId);
    if (!target) {
      throw new Error("Không tìm thấy rule thông báo");
    }
    target.is_active = isActive;
    target.updated_at = new Date().toISOString();
    return target;
  }

  return apiAuthPatch<NotificationRule>(
    `/api/v1/notifications/rules/${ruleId}/toggle`,
    { is_active: isActive },
  );
}

export async function deleteNotificationRule(ruleId: number): Promise<void> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    mockRules = mockRules.filter((rule) => rule.id !== ruleId);
    return;
  }

  await apiAuthDelete(`/api/v1/notifications/rules/${ruleId}`);
}

export async function getNotificationSettings(): Promise<NotificationSettings> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    return mockSettings;
  }

  return apiAuthGet<NotificationSettings>("/api/v1/notifications/settings");
}

export async function updateNotificationSettings(
  payload: NotificationSettingsUpdatePayload,
): Promise<NotificationSettings> {
  if (ENABLE_MOCK_API) {
    await delay(MOCK_API_DELAY_MS);
    mockSettings = {
      ...mockSettings,
      ...payload,
    };
    return mockSettings;
  }

  return apiAuthPatch<NotificationSettings>("/api/v1/notifications/settings", payload);
}
