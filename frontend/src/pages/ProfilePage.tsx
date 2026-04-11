import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { useAuth } from "@/contexts/AuthContext";
import { changePassword } from "@/lib/authApi";
import { NotificationRulesCard } from "@/components/notifications/NotificationRulesCard";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  useUpdateNotificationSettingsMutation,
} from "@/hooks/useNotificationMutations";
import { useNotificationSettingsQuery } from "@/hooks/useNotificationsQuery";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { toast } from "sonner";
import { User, Lock, ShieldCheck } from "lucide-react";
import { format } from "date-fns";
import { vi } from "date-fns/locale";

// ---------------------------------------------------------------------------
// Zod schemas
// ---------------------------------------------------------------------------

const profileSchema = z.object({
  username: z.string().min(3, "Tối thiểu 3 ký tự").max(50),
  email: z.string().email("Email không hợp lệ"),
  full_name: z.string().optional(),
});

const passwordSchema = z
  .object({
    current_password: z.string().min(1, "Nhập mật khẩu hiện tại"),
    new_password: z.string().min(8, "Tối thiểu 8 ký tự").max(100),
    confirm_password: z.string(),
  })
  .refine((d) => d.new_password === d.confirm_password, {
    message: "Mật khẩu xác nhận không khớp",
    path: ["confirm_password"],
  });

type ProfileValues = z.infer<typeof profileSchema>;
type PasswordValues = z.infer<typeof passwordSchema>;

function getUserInitials(user?: {
  full_name?: string | null;
  username?: string | null;
}): string {
  if (user?.full_name) {
    return user.full_name
      .split(" ")
      .map((word) => word[0])
      .join("")
      .slice(0, 2)
      .toUpperCase();
  }

  return user?.username?.slice(0, 2).toUpperCase() ?? "U";
}

function formatDateLabel(value?: string | null): string {
  if (!value) {
    return "--";
  }

  return format(new Date(value), "dd/MM/yyyy", { locale: vi });
}

function formatDateTimeLabel(value?: string | null): string {
  if (!value) {
    return "--";
  }

  return format(new Date(value), "dd/MM/yyyy HH:mm", { locale: vi });
}

// ---------------------------------------------------------------------------
// Profile info form
// ---------------------------------------------------------------------------

function ProfileInfoCard() {
  const { user, updateProfile } = useAuth();
  const [loading, setLoading] = useState(false);

  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isDirty },
  } = useForm<ProfileValues>({
    resolver: zodResolver(profileSchema),
    defaultValues: {
      username: user?.username ?? "",
      email: user?.email ?? "",
      full_name: user?.full_name ?? "",
    },
  });

  // Sync khi user thay đổi (vd sau khi lưu)
  useEffect(() => {
    if (user) {
      reset({
        username: user.username,
        email: user.email,
        full_name: user.full_name ?? "",
      });
    }
  }, [user, reset]);

  async function onSubmit(values: ProfileValues) {
    setLoading(true);
    try {
      await updateProfile({
        username: values.username,
        email: values.email,
        full_name: values.full_name || undefined,
      });
      toast.success("Cập nhật thông tin thành công");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Cập nhật thất bại");
    } finally {
      setLoading(false);
    }
  }

  const initials = getUserInitials(user);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-3">
          <User className="h-5 w-5 text-muted-foreground" />
          <div>
            <CardTitle className="text-base">Thông tin cá nhân</CardTitle>
            <CardDescription>Cập nhật tên, email và tên hiển thị</CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Avatar */}
        <div className="flex items-center gap-4">
          <div className="h-16 w-16 rounded-full bg-primary/20 flex items-center justify-center">
            <span className="text-xl font-bold text-primary">{initials}</span>
          </div>
          <div>
            <p className="font-medium">{user?.full_name ?? user?.username}</p>
            <p className="text-sm text-muted-foreground">{user?.email}</p>
            {user?.created_at && (
              <p className="text-xs text-muted-foreground mt-0.5">
                Tham gia{" "}
                {format(new Date(user.created_at), "dd/MM/yyyy", { locale: vi })}
              </p>
            )}
          </div>
        </div>

        <Separator />

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="username">Tên đăng nhập</Label>
              <Input id="username" {...register("username")} />
              {errors.username && (
                <p className="text-xs text-destructive">{errors.username.message}</p>
              )}
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="email">Email</Label>
              <Input id="email" type="email" {...register("email")} />
              {errors.email && (
                <p className="text-xs text-destructive">{errors.email.message}</p>
              )}
            </div>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="full_name">
              Họ tên{" "}
              <span className="text-muted-foreground text-xs">(tuỳ chọn)</span>
            </Label>
            <Input id="full_name" placeholder="Nguyễn Văn A" {...register("full_name")} />
          </div>

          <div className="flex justify-end">
            <Button type="submit" disabled={loading || !isDirty} className="min-w-28">
              {loading ? "Đang lưu..." : "Lưu thay đổi"}
            </Button>
          </div>
        </form>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Change password form
// ---------------------------------------------------------------------------

function ChangePasswordCard() {
  const [loading, setLoading] = useState(false);

  const {
    register,
    handleSubmit,
    reset,
    formState: { errors },
  } = useForm<PasswordValues>({ resolver: zodResolver(passwordSchema) });

  async function onSubmit(values: PasswordValues) {
    setLoading(true);
    try {
      await changePassword({
        current_password: values.current_password,
        new_password: values.new_password,
      });
      toast.success("Đổi mật khẩu thành công");
      reset();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Đổi mật khẩu thất bại");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-3">
          <Lock className="h-5 w-5 text-muted-foreground" />
          <div>
            <CardTitle className="text-base">Đổi mật khẩu</CardTitle>
            <CardDescription>
              Mật khẩu mới tối thiểu 8 ký tự
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="current_password">Mật khẩu hiện tại</Label>
            <Input
              id="current_password"
              type="password"
              placeholder="••••••••"
              autoComplete="current-password"
              {...register("current_password")}
            />
            {errors.current_password && (
              <p className="text-xs text-destructive">
                {errors.current_password.message}
              </p>
            )}
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="new_password">Mật khẩu mới</Label>
              <Input
                id="new_password"
                type="password"
                placeholder="••••••••"
                autoComplete="new-password"
                {...register("new_password")}
              />
              {errors.new_password && (
                <p className="text-xs text-destructive">
                  {errors.new_password.message}
                </p>
              )}
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="confirm_password">Xác nhận mật khẩu mới</Label>
              <Input
                id="confirm_password"
                type="password"
                placeholder="••••••••"
                autoComplete="new-password"
                {...register("confirm_password")}
              />
              {errors.confirm_password && (
                <p className="text-xs text-destructive">
                  {errors.confirm_password.message}
                </p>
              )}
            </div>
          </div>

          <div className="flex justify-end">
            <Button type="submit" disabled={loading} className="min-w-36">
              {loading ? "Đang cập nhật..." : "Đổi mật khẩu"}
            </Button>
          </div>
        </form>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Notification settings card
// ---------------------------------------------------------------------------

function NotificationSettingsCard() {
  const { data: settings, isLoading } = useNotificationSettingsQuery();
  const updateSettingsMutation = useUpdateNotificationSettingsMutation();

  async function handleToggle(
    field:
      | "notifications_enabled"
      | "notification_price_enabled"
      | "notification_news_enabled"
      | "notification_portfolio_enabled",
    checked: boolean,
  ) {
    try {
      await updateSettingsMutation.mutateAsync({ [field]: checked });
      toast.success("Đã cập nhật cài đặt thông báo");
    } catch (err) {
      toast.error(
        err instanceof Error ? err.message : "Cập nhật cài đặt thông báo thất bại",
      );
    }
  }

  const masterEnabled = settings?.notifications_enabled ?? true;
  const disabled = isLoading || updateSettingsMutation.isPending;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Cài đặt thông báo</CardTitle>
        <CardDescription>
          Tùy chỉnh thông báo trong ứng dụng theo từng nhóm sự kiện
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between rounded-lg border p-3">
          <div>
            <p className="text-sm font-medium">Bật thông báo</p>
            <p className="text-xs text-muted-foreground">
              Tắt sẽ ẩn toàn bộ thông báo trong app
            </p>
          </div>
          <Switch
            checked={masterEnabled}
            disabled={disabled}
            onCheckedChange={(checked) =>
              handleToggle("notifications_enabled", checked)
            }
          />
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between rounded-lg border p-3">
            <div>
              <p className="text-sm font-medium">Thông báo giá</p>
              <p className="text-xs text-muted-foreground">
                Nhận cảnh báo khi giá vượt ngưỡng hoặc biến động mạnh
              </p>
            </div>
            <Switch
              checked={settings?.notification_price_enabled ?? true}
              disabled={disabled || !masterEnabled}
              onCheckedChange={(checked) =>
                handleToggle("notification_price_enabled", checked)
              }
            />
          </div>

          <div className="flex items-center justify-between rounded-lg border p-3">
            <div>
              <p className="text-sm font-medium">Thông báo tin tức</p>
              <p className="text-xs text-muted-foreground">
                Nhận thông báo khi có tin mới liên quan mã theo dõi
              </p>
            </div>
            <Switch
              checked={settings?.notification_news_enabled ?? true}
              disabled={disabled || !masterEnabled}
              onCheckedChange={(checked) =>
                handleToggle("notification_news_enabled", checked)
              }
            />
          </div>

          <div className="flex items-center justify-between rounded-lg border p-3">
            <div>
              <p className="text-sm font-medium">Thông báo danh mục</p>
              <p className="text-xs text-muted-foreground">
                Nhận cập nhật liên quan thay đổi danh mục và sự kiện hệ thống
              </p>
            </div>
            <Switch
              checked={settings?.notification_portfolio_enabled ?? true}
              disabled={disabled || !masterEnabled}
              onCheckedChange={(checked) =>
                handleToggle("notification_portfolio_enabled", checked)
              }
            />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Account info card (read-only)
// ---------------------------------------------------------------------------

function AccountStatusCard() {
  const { user } = useAuth();

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-3">
          <ShieldCheck className="h-5 w-5 text-muted-foreground" />
          <div>
            <CardTitle className="text-base">Trạng thái tài khoản</CardTitle>
            <CardDescription>Thông tin bảo mật tài khoản</CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <dl className="space-y-3 text-sm">
          <div className="flex justify-between items-center">
            <dt className="text-muted-foreground">Trạng thái</dt>
            <dd>
              <span
                className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium ${
                  user?.is_active
                    ? "bg-[#26a641]/15 text-[#26a641]"
                    : "bg-destructive/15 text-destructive"
                }`}
              >
                <span
                  className={`h-1.5 w-1.5 rounded-full ${
                    user?.is_active ? "bg-[#26a641]" : "bg-destructive"
                  }`}
                />
                {user?.is_active ? "Đang hoạt động" : "Bị khoá"}
              </span>
            </dd>
          </div>
          <Separator />
          <div className="flex justify-between items-center">
            <dt className="text-muted-foreground">Xác minh email</dt>
            <dd>
              <span
                className={`text-xs font-medium ${
                  user?.is_verified ? "text-[#26a641]" : "text-yellow-500"
                }`}
              >
                {user?.is_verified ? "Đã xác minh" : "Chưa xác minh"}
              </span>
            </dd>
          </div>
          <Separator />
          <div className="flex justify-between items-center">
            <dt className="text-muted-foreground">Ngày tham gia</dt>
            <dd className="text-foreground">
              {user?.created_at ? formatDateLabel(user.created_at) : "—"}
            </dd>
          </div>
          <Separator />
          <div className="flex justify-between items-center">
            <dt className="text-muted-foreground">Cập nhật lần cuối</dt>
            <dd className="text-foreground">
              {user?.updated_at ? formatDateTimeLabel(user.updated_at) : "—"}
            </dd>
          </div>
        </dl>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function ProfilePage() {
  const { user } = useAuth();
  const initials = getUserInitials(user);

  return (
    <div className="mx-auto w-full max-w-7xl space-y-6">
      <section className="relative overflow-hidden rounded-2xl border border-border/70 bg-gradient-to-br from-card to-accent/30 p-5 md:p-6">
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(38,166,65,0.18),transparent_58%)]" />

        <div className="relative flex flex-col gap-6">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div className="flex items-center gap-4">
              <div className="flex h-16 w-16 shrink-0 items-center justify-center rounded-full bg-primary/20 ring-4 ring-background/60">
                <span className="text-xl font-bold text-primary">{initials}</span>
              </div>

              <div>
                <h1 className="text-xl font-semibold tracking-tight">Hồ sơ cá nhân</h1>
                <p className="text-sm text-muted-foreground mt-1">
                  Quản lý thông tin tài khoản, bảo mật và thông báo tại một nơi
                </p>
              </div>
            </div>

            <div
              className={`inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-xs font-medium ${
                user?.is_active
                  ? "bg-[#26a641]/15 text-[#26a641]"
                  : "bg-destructive/15 text-destructive"
              }`}
            >
              <span
                className={`h-1.5 w-1.5 rounded-full ${
                  user?.is_active ? "bg-[#26a641]" : "bg-destructive"
                }`}
              />
              {user?.is_active ? "Tài khoản đang hoạt động" : "Tài khoản bị khoá"}
            </div>
          </div>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <div className="rounded-xl border border-border/70 bg-background/40 p-3">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Người dùng</p>
              <p className="mt-1 text-sm font-medium">{user?.full_name ?? user?.username ?? "--"}</p>
              <p className="text-xs text-muted-foreground">@{user?.username ?? "--"}</p>
            </div>

            <div className="rounded-xl border border-border/70 bg-background/40 p-3">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Email</p>
              <p className="mt-1 text-sm font-medium truncate">{user?.email ?? "--"}</p>
              <p className={`text-xs ${user?.is_verified ? "text-[#26a641]" : "text-yellow-500"}`}>
                {user?.is_verified ? "Đã xác minh" : "Chưa xác minh"}
              </p>
            </div>

            <div className="rounded-xl border border-border/70 bg-background/40 p-3">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Ngày tham gia</p>
              <p className="mt-1 text-sm font-medium">{formatDateLabel(user?.created_at)}</p>
              <p className="text-xs text-muted-foreground">Theo múi giờ hệ thống</p>
            </div>

            <div className="rounded-xl border border-border/70 bg-background/40 p-3">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Cập nhật gần nhất</p>
              <p className="mt-1 text-sm font-medium">{formatDateTimeLabel(user?.updated_at)}</p>
              <p className="text-xs text-muted-foreground">Đồng bộ thông tin tài khoản</p>
            </div>
          </div>
        </div>
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(300px,1fr)_minmax(0,2fr)]">
        <div className="space-y-6 xl:sticky xl:top-6 self-start">
          <AccountStatusCard />
          <NotificationSettingsCard />
        </div>

        <div className="space-y-6">
          <ProfileInfoCard />
          <ChangePasswordCard />
          <NotificationRulesCard />
        </div>
      </div>
    </div>
  );
}
