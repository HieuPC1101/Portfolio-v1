import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import {
  clearTokens,
  getAccessToken,
  getMe,
  getRefreshToken,
  login as apiLogin,
  logout as apiLogout,
  refreshTokenApi,
  register as apiRegister,
  saveTokens,
  updateProfile as apiUpdateProfile,
} from "@/lib/authApi";
import type { LoginFormData, RegisterFormData, UpdateProfileData, User } from "@/types/auth";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface AuthState {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
}

interface AuthContextValue extends AuthState {
  login: (data: LoginFormData) => Promise<void>;
  logout: () => Promise<void>;
  register: (data: RegisterFormData) => Promise<void>;
  updateProfile: (data: UpdateProfileData) => Promise<void>;
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

const AuthContext = createContext<AuthContextValue | null>(null);

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<AuthState>({
    user: null,
    isLoading: true,
    isAuthenticated: false,
  });

  // Kiểm tra token lưu sẵn khi app khởi động
  useEffect(() => {
    let cancelled = false;

    async function initAuth() {
      const accessToken = getAccessToken();
      if (!accessToken) {
        setState({ user: null, isLoading: false, isAuthenticated: false });
        return;
      }

      try {
        const user = await getMe(accessToken);
        if (!cancelled) {
          setState({ user, isLoading: false, isAuthenticated: true });
        }
      } catch {
        // Access token hết hạn → thử refresh
        const refreshToken = getRefreshToken();
        if (!refreshToken) {
          clearTokens();
          if (!cancelled) setState({ user: null, isLoading: false, isAuthenticated: false });
          return;
        }

        try {
          const newToken = await refreshTokenApi(refreshToken);
          saveTokens(newToken);
          const user = await getMe(newToken.access_token);
          if (!cancelled) {
            setState({ user, isLoading: false, isAuthenticated: true });
          }
        } catch {
          clearTokens();
          if (!cancelled) setState({ user: null, isLoading: false, isAuthenticated: false });
        }
      }
    }

    initAuth();
    return () => { cancelled = true; };
  }, []);

  const login = useCallback(async (data: LoginFormData) => {
    const token = await apiLogin(data);
    saveTokens(token);
    const user = await getMe(token.access_token);
    setState({ user, isLoading: false, isAuthenticated: true });
  }, []);

  const logout = useCallback(async () => {
    const refreshToken = getRefreshToken();
    if (refreshToken) {
      await apiLogout(refreshToken);
    }
    clearTokens();
    setState({ user: null, isLoading: false, isAuthenticated: false });
  }, []);

  const register = useCallback(async (data: RegisterFormData) => {
    await apiRegister(data);
    // Tự động đăng nhập sau khi đăng ký
    await login({ username: data.username, password: data.password });
  }, [login]);

  const updateProfile = useCallback(async (data: UpdateProfileData) => {
    const updatedUser = await apiUpdateProfile(data);
    setState((prev) => ({ ...prev, user: updatedUser }));
  }, []);

  return (
    <AuthContext.Provider value={{ ...state, login, logout, register, updateProfile }}>
      {children}
    </AuthContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
