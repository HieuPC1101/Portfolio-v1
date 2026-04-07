import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { useAuth } from "@/contexts/AuthContext";
import { changePassword } from "@/lib/authApi";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
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

  const initials = user?.full_name
    ? user.full_name
        .split(" ")
        .map((w) => w[0])
        .join("")
        .slice(0, 2)
        .toUpperCase()
    : user?.username?.slice(0, 2).toUpperCase() ?? "U";

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
              {user?.created_at
                ? format(new Date(user.created_at), "dd/MM/yyyy", { locale: vi })
                : "—"}
            </dd>
          </div>
          <Separator />
          <div className="flex justify-between items-center">
            <dt className="text-muted-foreground">Cập nhật lần cuối</dt>
            <dd className="text-foreground">
              {user?.updated_at
                ? format(new Date(user.updated_at), "dd/MM/yyyy HH:mm", { locale: vi })
                : "—"}
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
  return (
    <div className="max-w-2xl mx-auto space-y-6 p-4 md:p-6">
      <div>
        <h1 className="text-xl font-semibold">Hồ sơ cá nhân</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Quản lý thông tin tài khoản và bảo mật
        </p>
      </div>

      <ProfileInfoCard />
      <ChangePasswordCard />
      <AccountStatusCard />
    </div>
  );
}
