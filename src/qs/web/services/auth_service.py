from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Any

from fastapi import Request


SESSION_USER_KEY = "qs_user"
DEFAULT_ADMIN_USERNAME = "pp"
DEFAULT_ADMIN_DISPLAY_NAME = "pp"
DEFAULT_ADMIN_ROLE = "admin"
DEFAULT_ADMIN_PASSWORD_HASH = (
    "pbkdf2_sha256$240000$a699995c80d093c198fdf299bd321a68$"
    "d137719323d8fe3ff5167b22aa21573d17f4a5f3d52d8aada7da27053bdcf70d"
)


def hash_password(password: str, *, iterations: int = 240_000, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    ).hex()
    return f"pbkdf2_sha256${iterations}${salt}${digest}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations_text, salt, expected_digest = password_hash.split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    try:
        iterations = int(iterations_text)
    except ValueError:
        return False
    actual_hash = hash_password(password, iterations=iterations, salt=salt)
    return hmac.compare_digest(actual_hash, password_hash)


class AuthService:
    def __init__(self, repo):
        self.repo = repo

    def ensure_default_admin(self) -> None:
        self.repo.upsert_user(
            username=DEFAULT_ADMIN_USERNAME,
            password_hash=DEFAULT_ADMIN_PASSWORD_HASH,
            role=DEFAULT_ADMIN_ROLE,
            display_name=DEFAULT_ADMIN_DISPLAY_NAME,
            is_active=True,
        )

    def authenticate(self, username: str, password: str) -> dict[str, Any] | None:
        user = self.repo.get_user_by_username(username)
        if user is None or not user["is_active"]:
            return None
        if not verify_password(password, user["password_hash"]):
            return None
        self.repo.touch_user_login(username)
        return self._to_public_user(user)

    def login(self, request: Request, user: dict[str, Any]) -> None:
        request.session[SESSION_USER_KEY] = {
            "username": user["username"],
            "role": user["role"],
        }

    def logout(self, request: Request) -> None:
        request.session.pop(SESSION_USER_KEY, None)

    def get_current_user(self, request: Request) -> dict[str, Any] | None:
        session_user = request.session.get(SESSION_USER_KEY)
        if not isinstance(session_user, dict):
            return None
        username = str(session_user.get("username") or "").strip()
        if not username:
            return None
        user = self.repo.get_user_by_username(username)
        if user is None or not user["is_active"]:
            self.logout(request)
            return None
        return self._to_public_user(user)

    @staticmethod
    def is_admin(user: dict[str, Any] | None) -> bool:
        return bool(user and user.get("role") == DEFAULT_ADMIN_ROLE)

    def _to_public_user(self, user: dict[str, Any]) -> dict[str, Any]:
        return {
            "username": user["username"],
            "display_name": user["display_name"],
            "role": user["role"],
            "is_active": user["is_active"],
        }
