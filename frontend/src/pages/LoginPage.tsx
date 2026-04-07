import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { toast } from "sonner";
import { TrendingUp } from "lucide-react";

// ---------------------------------------------------------------------------
// Zod schemas
// ---------------------------------------------------------------------------

const loginSchema = z.object({
  username: z.string().min(3, "Tên đăng nhập tối thiểu 3 ký tự"),
  password: z.string().min(8, "Mật khẩu tối thiểu 8 ký tự"),
});

const registerSchema = z.object({
  username: z.string().min(3, "Tên đăng nhập tối thiểu 3 ký tự").max(50),
  email: z.string().email("Email không hợp lệ"),
  full_name: z.string().optional(),
  password: z.string().min(8, "Mật khẩu tối thiểu 8 ký tự").max(100),
});

type LoginValues = z.infer<typeof loginSchema>;
type RegisterValues = z.infer<typeof registerSchema>;

// ---------------------------------------------------------------------------
// Login form
// ---------------------------------------------------------------------------

function LoginForm({ onSuccess }: { onSuccess: () => void }) {
  const { login } = useAuth();
  const [loading, setLoading] = useState(false);

  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<LoginValues>({ resolver: zodResolver(loginSchema) });

  async function onSubmit(values: LoginValues) {
    setLoading(true);
    try {
      await login(values);
      onSuccess();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Đăng nhập thất bại");
    } finally {
      setLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
      <div className="space-y-1.5">
        <Label htmlFor="login-username">Tên đăng nhập</Label>
        <Input
          id="login-username"
          placeholder="username"
          autoComplete="username"
          {...register("username")}
        />
        {errors.username && (
          <p className="text-xs text-destructive">{errors.username.message}</p>
        )}
      </div>

      <div className="space-y-1.5">
        <Label htmlFor="login-password">Mật khẩu</Label>
        <Input
          id="login-password"
          type="password"
          placeholder="••••••••"
          autoComplete="current-password"
          {...register("password")}
        />
        {errors.password && (
          <p className="text-xs text-destructive">{errors.password.message}</p>
        )}
      </div>

      <Button type="submit" className="w-full" disabled={loading}>
        {loading ? "Đang đăng nhập..." : "Đăng nhập"}
      </Button>
    </form>
  );
}

// ---------------------------------------------------------------------------
// Register form
// ---------------------------------------------------------------------------

function RegisterForm({ onSuccess }: { onSuccess: () => void }) {
  const { register: authRegister } = useAuth();
  const [loading, setLoading] = useState(false);

  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<RegisterValues>({ resolver: zodResolver(registerSchema) });

  async function onSubmit(values: RegisterValues) {
    setLoading(true);
    try {
      await authRegister(values);
      toast.success("Đăng ký thành công!");
      onSuccess();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Đăng ký thất bại");
    } finally {
      setLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
      <div className="space-y-1.5">
        <Label htmlFor="reg-username">Tên đăng nhập</Label>
        <Input
          id="reg-username"
          placeholder="username"
          autoComplete="username"
          {...register("username")}
        />
        {errors.username && (
          <p className="text-xs text-destructive">{errors.username.message}</p>
        )}
      </div>

      <div className="space-y-1.5">
        <Label htmlFor="reg-email">Email</Label>
        <Input
          id="reg-email"
          type="email"
          placeholder="you@example.com"
          autoComplete="email"
          {...register("email")}
        />
        {errors.email && (
          <p className="text-xs text-destructive">{errors.email.message}</p>
        )}
      </div>

      <div className="space-y-1.5">
        <Label htmlFor="reg-fullname">
          Họ tên{" "}
          <span className="text-muted-foreground text-xs">(tuỳ chọn)</span>
        </Label>
        <Input
          id="reg-fullname"
          placeholder="Nguyễn Văn A"
          autoComplete="name"
          {...register("full_name")}
        />
      </div>

      <div className="space-y-1.5">
        <Label htmlFor="reg-password">Mật khẩu</Label>
        <Input
          id="reg-password"
          type="password"
          placeholder="••••••••"
          autoComplete="new-password"
          {...register("password")}
        />
        {errors.password && (
          <p className="text-xs text-destructive">{errors.password.message}</p>
        )}
      </div>

      <Button type="submit" className="w-full" disabled={loading}>
        {loading ? "Đang tạo tài khoản..." : "Tạo tài khoản"}
      </Button>
    </form>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function LoginPage() {
  const { isAuthenticated } = useAuth();
  const navigate = useNavigate();

  // Nếu đã đăng nhập thì chuyển về dashboard
  if (isAuthenticated) {
    navigate("/", { replace: true });
    return null;
  }

  function handleSuccess() {
    navigate("/", { replace: true });
  }

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Logo */}
        <div className="flex items-center justify-center gap-2 mb-8">
          <div className="h-9 w-9 rounded-lg bg-primary flex items-center justify-center">
            <TrendingUp className="h-5 w-5 text-primary-foreground" />
          </div>
          <span className="text-2xl font-bold tracking-tight">Finstock</span>
        </div>

        <Card className="border-border">
          <CardHeader className="pb-4">
            <CardTitle className="text-center text-lg">Chào mừng trở lại</CardTitle>
            <CardDescription className="text-center">
              Quản lý danh mục chứng khoán của bạn
            </CardDescription>
          </CardHeader>

          <CardContent>
            <Tabs defaultValue="login">
              <TabsList className="w-full mb-6">
                <TabsTrigger value="login" className="flex-1">
                  Đăng nhập
                </TabsTrigger>
                <TabsTrigger value="register" className="flex-1">
                  Đăng ký
                </TabsTrigger>
              </TabsList>

              <TabsContent value="login">
                <LoginForm onSuccess={handleSuccess} />
              </TabsContent>

              <TabsContent value="register">
                <RegisterForm onSuccess={handleSuccess} />
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
