import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import Index from "./pages/Index";
import NotFound from "./pages/NotFound";
import LoginPage from "./pages/LoginPage";
import ProfilePage from "./pages/ProfilePage";
import { MainLayout } from "@/components/layout/MainLayout";
import { AuthProvider } from "@/contexts/AuthContext";
import { ProtectedRoute } from "@/components/auth/ProtectedRoute";
import { lazy, Suspense } from "react";
import { LoadingSpinner } from "@/components/common/LoadingSpinner";

const PortfolioPage = lazy(() => import("./pages/PortfolioPage"));
const OptimizePage = lazy(() => import("./pages/OptimizePage"));
const MarketPage = lazy(() => import("./pages/MarketPage"));
const NewsPage = lazy(() => import("./pages/NewsPage"));
const StockDetailPage = lazy(() => import("./pages/StockDetailPage"));

const queryClient = new QueryClient();

function LayoutPage({ children }: { children: React.ReactNode }) {
  return (
    <ProtectedRoute>
      <MainLayout>
        <Suspense fallback={<div className="flex items-center justify-center h-64"><LoadingSpinner /></div>}>
          {children}
        </Suspense>
      </MainLayout>
    </ProtectedRoute>
  );
}

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <AuthProvider>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/" element={<LayoutPage><Index /></LayoutPage>} />
            <Route path="/danh-muc" element={<LayoutPage><PortfolioPage /></LayoutPage>} />
            <Route path="/toi-uu" element={<LayoutPage><OptimizePage /></LayoutPage>} />
            <Route path="/thi-truong" element={<LayoutPage><MarketPage /></LayoutPage>} />
            <Route path="/co-phieu/:symbol" element={<LayoutPage><StockDetailPage /></LayoutPage>} />
            <Route path="/tin-tuc" element={<LayoutPage><NewsPage /></LayoutPage>} />
            <Route path="/ho-so" element={<LayoutPage><ProfilePage /></LayoutPage>} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </AuthProvider>
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
