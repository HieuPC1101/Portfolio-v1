import { MainLayout } from "@/components/layout/MainLayout";

// Wrapper to add MainLayout to each page
function withLayout(Component: React.ComponentType) {
  return function LayoutWrapped() {
    return (
      <MainLayout>
        <Component />
      </MainLayout>
    );
  };
}

export { withLayout };
