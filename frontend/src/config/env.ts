export const API_MODE = import.meta.env.VITE_API_MODE ?? "mock";
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";
export const MOCK_DELAY_MS = Number(import.meta.env.VITE_MOCK_DELAY_MS ?? 400);
export const AUTH_TOKEN = import.meta.env.VITE_AUTH_TOKEN ?? "";

export const isMockMode = API_MODE === "mock";
