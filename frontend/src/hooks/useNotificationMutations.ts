import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  createNotificationRule,
  deleteNotificationRule,
  markAllNotificationsAsRead,
  markNotificationAsRead,
  toggleNotificationRule,
  updateNotificationRule,
  updateNotificationSettings,
} from "@/repositories/notificationRepository";
import type {
  NotificationRuleCreatePayload,
  NotificationRuleUpdatePayload,
  NotificationSettingsUpdatePayload,
} from "@/types/notification";

function useNotificationInvalidator() {
  const queryClient = useQueryClient();

  return () => {
    queryClient.invalidateQueries({ queryKey: ["notifications"] });
  };
}

export function useMarkNotificationReadMutation() {
  const invalidate = useNotificationInvalidator();

  return useMutation({
    mutationFn: markNotificationAsRead,
    onSuccess: () => invalidate(),
  });
}

export function useMarkAllNotificationsReadMutation() {
  const invalidate = useNotificationInvalidator();

  return useMutation({
    mutationFn: markAllNotificationsAsRead,
    onSuccess: () => invalidate(),
  });
}

export function useUpdateNotificationSettingsMutation() {
  const invalidate = useNotificationInvalidator();

  return useMutation({
    mutationFn: (payload: NotificationSettingsUpdatePayload) =>
      updateNotificationSettings(payload),
    onSuccess: () => invalidate(),
  });
}

export function useCreateNotificationRuleMutation() {
  const invalidate = useNotificationInvalidator();

  return useMutation({
    mutationFn: (payload: NotificationRuleCreatePayload) =>
      createNotificationRule(payload),
    onSuccess: () => invalidate(),
  });
}

export function useUpdateNotificationRuleMutation() {
  const invalidate = useNotificationInvalidator();

  return useMutation({
    mutationFn: ({
      ruleId,
      payload,
    }: {
      ruleId: number;
      payload: NotificationRuleUpdatePayload;
    }) => updateNotificationRule(ruleId, payload),
    onSuccess: () => invalidate(),
  });
}

export function useToggleNotificationRuleMutation() {
  const invalidate = useNotificationInvalidator();

  return useMutation({
    mutationFn: ({ ruleId, isActive }: { ruleId: number; isActive: boolean }) =>
      toggleNotificationRule(ruleId, isActive),
    onSuccess: () => invalidate(),
  });
}

export function useDeleteNotificationRuleMutation() {
  const invalidate = useNotificationInvalidator();

  return useMutation({
    mutationFn: deleteNotificationRule,
    onSuccess: () => invalidate(),
  });
}
