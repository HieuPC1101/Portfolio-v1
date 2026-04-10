import { useMemo, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  useCreateNotificationRuleMutation,
  useDeleteNotificationRuleMutation,
  useToggleNotificationRuleMutation,
  useUpdateNotificationRuleMutation,
} from "@/hooks/useNotificationMutations";
import { useNotificationRulesQuery } from "@/hooks/useNotificationsQuery";
import type {
  NotificationOperator,
  NotificationRule,
  NotificationRuleType,
} from "@/types/notification";
import { Pencil, Plus, Trash2 } from "lucide-react";

type RuleFormState = {
  rule_type: NotificationRuleType;
  symbol: string;
  operator: NotificationOperator;
  threshold_value: string;
  cooldown_minutes: string;
  is_active: boolean;
};

const RULE_TYPE_OPTIONS: Array<{ value: NotificationRuleType; label: string }> = [
  { value: "price_cross", label: "Giá vượt ngưỡng" },
  { value: "price_change_percent", label: "Biến động phần trăm" },
  { value: "news_watchlist", label: "Tin tức theo watchlist" },
];

const OPERATOR_OPTIONS: Array<{ value: NotificationOperator; label: string }> = [
  { value: "cross_up", label: "Cắt lên ngưỡng" },
  { value: "cross_down", label: "Cắt xuống ngưỡng" },
  { value: "gt", label: "Lớn hơn" },
  { value: "gte", label: "Lớn hơn hoặc bằng" },
  { value: "lt", label: "Nhỏ hơn" },
  { value: "lte", label: "Nhỏ hơn hoặc bằng" },
];

const DEFAULT_FORM: RuleFormState = {
  rule_type: "price_cross",
  symbol: "",
  operator: "cross_up",
  threshold_value: "",
  cooldown_minutes: "60",
  is_active: true,
};

function getRuleTypeLabel(ruleType: NotificationRuleType): string {
  return RULE_TYPE_OPTIONS.find((item) => item.value === ruleType)?.label ?? ruleType;
}

function normalizeRuleForm(form: RuleFormState) {
  const symbol = form.symbol.trim().toUpperCase();
  const cooldown = Number.parseInt(form.cooldown_minutes, 10);
  const threshold = Number.parseFloat(form.threshold_value);
  const isNewsRule = form.rule_type === "news_watchlist";

  if (!isNewsRule && !symbol) {
    throw new Error("Vui lòng nhập mã cổ phiếu");
  }
  if (!Number.isFinite(cooldown) || cooldown <= 0) {
    throw new Error("Cooldown phải là số nguyên dương");
  }
  if (!isNewsRule && !Number.isFinite(threshold)) {
    throw new Error("Vui lòng nhập ngưỡng hợp lệ");
  }

  return {
    rule_type: form.rule_type,
    symbol: isNewsRule ? undefined : symbol,
    operator: isNewsRule ? undefined : form.operator,
    threshold_value: isNewsRule ? undefined : threshold,
    cooldown_minutes: cooldown,
    is_active: form.is_active,
  };
}

function normalizeRuleFormForUpdate(form: RuleFormState) {
  const base = normalizeRuleForm(form);
  if (form.rule_type !== "news_watchlist") {
    return base;
  }

  return {
    ...base,
    symbol: null,
    operator: null,
    threshold_value: null,
  };
}

function formFromRule(rule: NotificationRule): RuleFormState {
  return {
    rule_type: rule.rule_type,
    symbol: rule.symbol ?? "",
    operator: (rule.operator as NotificationOperator) ?? "cross_up",
    threshold_value:
      typeof rule.threshold_value === "number" ? String(rule.threshold_value) : "",
    cooldown_minutes: String(rule.cooldown_minutes),
    is_active: rule.is_active,
  };
}

interface RuleFormProps {
  value: RuleFormState;
  onChange: (next: RuleFormState) => void;
}

function RuleFormFields({ value, onChange }: RuleFormProps) {
  const isNewsRule = value.rule_type === "news_watchlist";

  return (
    <div className="space-y-3">
      <div className="grid gap-3 sm:grid-cols-2">
        <div className="space-y-1.5">
          <Label>Loại rule</Label>
          <Select
            value={value.rule_type}
            onValueChange={(ruleType) =>
              onChange({
                ...value,
                rule_type: ruleType as NotificationRuleType,
              })
            }
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {RULE_TYPE_OPTIONS.map((item) => (
                <SelectItem key={item.value} value={item.value}>
                  {item.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-1.5">
          <Label>Mã cổ phiếu</Label>
          <Input
            placeholder={isNewsRule ? "(tuỳ chọn)" : "VD: FPT"}
            value={value.symbol}
            disabled={isNewsRule}
            onChange={(event) =>
              onChange({
                ...value,
                symbol: event.target.value.toUpperCase(),
              })
            }
          />
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <div className="space-y-1.5 sm:col-span-2">
          <Label>Điều kiện</Label>
          <Select
            value={value.operator}
            disabled={isNewsRule}
            onValueChange={(operator) =>
              onChange({
                ...value,
                operator: operator as NotificationOperator,
              })
            }
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {OPERATOR_OPTIONS.map((item) => (
                <SelectItem key={item.value} value={item.value}>
                  {item.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-1.5">
          <Label>Ngưỡng</Label>
          <Input
            type="number"
            step="0.01"
            placeholder={isNewsRule ? "-" : "VD: 140000"}
            value={value.threshold_value}
            disabled={isNewsRule}
            onChange={(event) =>
              onChange({
                ...value,
                threshold_value: event.target.value,
              })
            }
          />
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-[1fr_auto] sm:items-center">
        <div className="space-y-1.5">
          <Label>Cooldown (phút)</Label>
          <Input
            type="number"
            min={1}
            value={value.cooldown_minutes}
            onChange={(event) =>
              onChange({
                ...value,
                cooldown_minutes: event.target.value,
              })
            }
          />
        </div>
        <div className="flex items-center justify-between gap-3 rounded-md border p-2 sm:mt-6">
          <span className="text-sm">Kích hoạt</span>
          <Switch
            checked={value.is_active}
            onCheckedChange={(checked) => onChange({ ...value, is_active: checked })}
          />
        </div>
      </div>
    </div>
  );
}

export function NotificationRulesCard() {
  const { data: rules = [], isLoading, error } = useNotificationRulesQuery();

  const createRuleMutation = useCreateNotificationRuleMutation();
  const updateRuleMutation = useUpdateNotificationRuleMutation();
  const toggleRuleMutation = useToggleNotificationRuleMutation();
  const deleteRuleMutation = useDeleteNotificationRuleMutation();

  const [createForm, setCreateForm] = useState<RuleFormState>(DEFAULT_FORM);
  const [editingRule, setEditingRule] = useState<NotificationRule | null>(null);
  const [editForm, setEditForm] = useState<RuleFormState>(DEFAULT_FORM);

  const hasRules = useMemo(() => rules.length > 0, [rules]);

  async function handleCreateRule() {
    try {
      const payload = normalizeRuleForm(createForm);
      await createRuleMutation.mutateAsync(payload);
      toast.success("Đã tạo quy tắc thông báo");
      setCreateForm(DEFAULT_FORM);
    } catch (errorValue) {
      toast.error(
        errorValue instanceof Error
          ? errorValue.message
          : "Không thể tạo quy tắc thông báo",
      );
    }
  }

  function handleOpenEdit(rule: NotificationRule) {
    setEditingRule(rule);
    setEditForm(formFromRule(rule));
  }

  async function handleSaveEditRule() {
    if (!editingRule) {
      return;
    }

    try {
      const normalizedPayload = normalizeRuleFormForUpdate(editForm);
      await updateRuleMutation.mutateAsync({
        ruleId: editingRule.id,
        payload: normalizedPayload,
      });
      toast.success("Đã cập nhật quy tắc");
      setEditingRule(null);
    } catch (errorValue) {
      toast.error(
        errorValue instanceof Error
          ? errorValue.message
          : "Không thể cập nhật quy tắc",
      );
    }
  }

  async function handleDeleteRule(ruleId: number) {
    try {
      await deleteRuleMutation.mutateAsync(ruleId);
      toast.success("Đã xoá quy tắc thông báo");
    } catch (errorValue) {
      toast.error(
        errorValue instanceof Error
          ? errorValue.message
          : "Không thể xoá quy tắc",
      );
    }
  }

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Quy tắc thông báo</CardTitle>
          <CardDescription>
            Tạo, bật/tắt và chỉnh sửa rule để hệ thống tự gửi cảnh báo
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          <div className="rounded-lg border p-4">
            <div className="mb-3 flex items-center justify-between">
              <p className="text-sm font-medium">Tạo quy tắc mới</p>
              <Plus className="h-4 w-4 text-muted-foreground" />
            </div>

            <RuleFormFields value={createForm} onChange={setCreateForm} />

            <div className="mt-4 flex justify-end">
              <Button
                type="button"
                onClick={handleCreateRule}
                disabled={createRuleMutation.isPending}
              >
                {createRuleMutation.isPending ? "Đang tạo..." : "Tạo rule"}
              </Button>
            </div>
          </div>

          <div className="space-y-2">
            <p className="text-sm font-medium">Danh sách rule hiện có</p>

            {isLoading ? (
              <p className="text-sm text-muted-foreground">Đang tải rule...</p>
            ) : error ? (
              <p className="text-sm text-destructive">
                Không thể tải danh sách rule. Vui lòng thử lại.
              </p>
            ) : !hasRules ? (
              <p className="text-sm text-muted-foreground">
                Bạn chưa có quy tắc nào.
              </p>
            ) : (
              <div className="space-y-2">
                {rules.map((rule) => (
                  <div
                    key={rule.id}
                    className="flex flex-col gap-3 rounded-lg border p-3 sm:flex-row sm:items-center sm:justify-between"
                  >
                    <div className="space-y-1">
                      <p className="text-sm font-medium">{getRuleTypeLabel(rule.rule_type)}</p>
                      <p className="text-xs text-muted-foreground">
                        {rule.symbol ? `${rule.symbol} · ` : ""}
                        {rule.operator ?? "-"}
                        {typeof rule.threshold_value === "number"
                          ? ` ${rule.threshold_value}`
                          : ""}
                        {` · Cooldown ${rule.cooldown_minutes} phút`}
                      </p>
                    </div>

                    <div className="flex items-center gap-2">
                      <div className="flex items-center gap-2 rounded-md border px-2 py-1">
                        <span className="text-xs text-muted-foreground">Bật</span>
                        <Switch
                          checked={rule.is_active}
                          onCheckedChange={(checked) =>
                            toggleRuleMutation.mutate({
                              ruleId: rule.id,
                              isActive: checked,
                            })
                          }
                        />
                      </div>

                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => handleOpenEdit(rule)}
                      >
                        <Pencil className="mr-1 h-3.5 w-3.5" />
                        Sửa
                      </Button>

                      <Button
                        type="button"
                        variant="destructive"
                        size="sm"
                        onClick={() => handleDeleteRule(rule.id)}
                      >
                        <Trash2 className="mr-1 h-3.5 w-3.5" />
                        Xoá
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      <Dialog open={editingRule !== null} onOpenChange={(open) => !open && setEditingRule(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Chỉnh sửa quy tắc</DialogTitle>
            <DialogDescription>
              Cập nhật điều kiện và cooldown cho rule thông báo.
            </DialogDescription>
          </DialogHeader>

          <RuleFormFields value={editForm} onChange={setEditForm} />

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setEditingRule(null)}>
              Huỷ
            </Button>
            <Button
              type="button"
              onClick={handleSaveEditRule}
              disabled={updateRuleMutation.isPending}
            >
              {updateRuleMutation.isPending ? "Đang lưu..." : "Lưu thay đổi"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
