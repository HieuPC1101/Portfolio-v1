function parseBooleanEnv(value: string | undefined, fallback = false): boolean {
  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") {
    return true;
  }

  if (normalized === "false" || normalized === "0" || normalized === "no") {
    return false;
  }

  return fallback;
}

function parseNumberEnv(value: string | undefined, fallback = 0): number {
  if (value === undefined) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export const ENABLE_MOCK_API = parseBooleanEnv(import.meta.env.VITE_ENABLE_MOCK_API, false);
export const MOCK_API_DELAY_MS = parseNumberEnv(import.meta.env.VITE_MOCK_API_DELAY_MS, 0);
