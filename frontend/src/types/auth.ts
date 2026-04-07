export interface User {
  id: number;
  username: string;
  email: string;
  full_name: string | null;
  is_active: boolean;
  is_verified: boolean;
  created_at: string;
  updated_at: string;
}

export interface Token {
  access_token: string;
  refresh_token: string;
  token_type: string;
}

export interface LoginFormData {
  username: string;
  password: string;
}

export interface RegisterFormData {
  username: string;
  email: string;
  password: string;
  full_name?: string;
}

export interface UpdateProfileData {
  username?: string;
  email?: string;
  full_name?: string;
}

export interface ChangePasswordData {
  current_password: string;
  new_password: string;
}
