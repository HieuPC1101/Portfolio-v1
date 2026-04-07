import { isMockMode, MOCK_DELAY_MS } from "@/config/env";
import { optimizeMock } from "@/mocks/optimize.mock";
import type { OptimizePageData } from "@/types/optimize";

async function delay(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function getOptimizeData(): Promise<OptimizePageData> {
  if (isMockMode) {
    await delay(MOCK_DELAY_MS);
    return optimizeMock;
  }

  return optimizeMock;
}
