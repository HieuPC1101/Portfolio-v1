import { SidebarProvider } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/layout/AppSidebar";
import { AppHeader } from "@/components/layout/AppHeader";
import { ChatPanel } from "@/components/chat/ChatPanel";
import { useState } from "react";
import { MessageCircle, X } from "lucide-react";
import { Button } from "@/components/ui/button";

interface MainLayoutProps {
  children: React.ReactNode;
}

export function MainLayout({ children }: MainLayoutProps) {
  const [chatOpen, setChatOpen] = useState(false);

  return (
    <SidebarProvider>
      <div className="min-h-screen flex w-full">
        <AppSidebar />
        <div className="flex-1 flex flex-col min-w-0">
          <AppHeader />
          <div className="flex flex-1 overflow-hidden">
            <main className="flex-1 overflow-y-auto p-4 md:p-6">
              {children}
            </main>
            {/* Chat panel - separate frame */}
            {chatOpen && (
              <div className="w-80 lg:w-96 border-l border-border flex flex-col bg-card shrink-0">
                <div className="h-14 flex items-center justify-between px-4 border-b border-border">
                  <span className="font-semibold text-sm">Trợ lý Finstock</span>
                  <Button variant="ghost" size="icon" onClick={() => setChatOpen(false)} className="h-7 w-7">
                    <X className="h-4 w-4" />
                  </Button>
                </div>
                <ChatPanel />
              </div>
            )}
          </div>
        </div>
        {/* Floating chat button */}
        {!chatOpen && (
          <Button
            onClick={() => setChatOpen(true)}
            className="fixed bottom-6 right-6 h-12 w-12 rounded-full shadow-lg bg-primary hover:bg-primary/90 z-50"
            size="icon"
          >
            <MessageCircle className="h-5 w-5" />
          </Button>
        )}
      </div>
    </SidebarProvider>
  );
}
