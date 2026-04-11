import { describe, expect, it, vi } from "vitest";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

import {
  createClientNotification,
  getNotifications,
  getNotificationSettings,
  getUnreadNotificationCount,
  markAllNotificationsAsRead,
  markNotificationAsRead,
  updateNotificationSettings,
} from "@/repositories/notificationRepository";

describe("notificationRepository (mock)", () => {
  it("tra ve danh sach thong bao va unread count", async () => {
    const list = await getNotifications({});
    const unread = await getUnreadNotificationCount();

    expect(list.items.length).toBeGreaterThan(0);
    expect(unread.unread_count).toBeGreaterThanOrEqual(0);
  });

  it("danh dau da doc cho mot thong bao", async () => {
    const before = await getNotifications({ status: "unread" });
    if (before.items.length === 0) {
      await markAllNotificationsAsRead();
      return;
    }

    const targetId = before.items[0].id;
    await markNotificationAsRead(targetId);

    const after = await getNotifications({ status: "unread" });
    expect(after.items.some((item) => item.id === targetId)).toBe(false);
  });

  it("cap nhat settings thong bao", async () => {
    const current = await getNotificationSettings();
    const nextValue = !current.notification_news_enabled;

    const updated = await updateNotificationSettings({
      notification_news_enabled: nextValue,
    });

    expect(updated.notification_news_enabled).toBe(nextValue);
  });

  it("them thong bao local cho hanh dong va hien trong trung tam thong bao", async () => {
    const before = await getUnreadNotificationCount();

    const created = createClientNotification({
      type: "portfolio_alert",
      title: "Đã thêm cổ phiếu",
      message: "Đã thêm VCB vào danh mục chính",
      payload: { source: "portfolio-tabs" },
    });

    expect(created.id).toBeLessThan(0);

    const unreadAfterCreate = await getUnreadNotificationCount();
    expect(unreadAfterCreate.unread_count).toBe(before.unread_count + 1);

    const unreadList = await getNotifications({ status: "unread" });
    expect(unreadList.items.some((item) => item.id === created.id)).toBe(true);

    await markNotificationAsRead(created.id);

    const unreadAfterRead = await getUnreadNotificationCount();
    expect(unreadAfterRead.unread_count).toBe(before.unread_count);
  });
});
