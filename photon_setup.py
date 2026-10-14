"""Photon 初始化向导的后端封装。

封装 photon.codes 的 Dashboard API 与 Spectrum API，让插件能在 WebUI 里完成
"设备登录 → 建项目 → 拿 Spectrum 凭据 → 注册手机号用户 → 拿 iMessage line"
整条初始化流程，完全不需要用户手动敲 CLI。

端点参考 hermes-agent 的 ``plugins/platforms/photon/auth.py``（已经真实验证）。
全部用 aiohttp 实现，无第三方 CLI 依赖。
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field

import aiohttp

logger = logging.getLogger("astrbot_plugin_photon_imessage")

# RFC 8628 device flow —— 官方 CLI 的 client_id（未开放注册独立 OAuth app，沿用此值）
DEFAULT_CLIENT_ID = "photon-cli"
DEFAULT_SCOPE = "user:read user:write"
DEFAULT_DASHBOARD_HOST = "https://app.photon.codes"
DEFAULT_SPECTRUM_HOST = "https://spectrum.photon.codes"
DEFAULT_DEVICE_POLL_TIMEOUT = 300  # 秒
DEFAULT_PROJECT_NAME = "astrbot-imessage"

E164_RE = re.compile(r"^\+\d{6,15}$")


@dataclass
class DeviceCode:
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str | None = None
    expires_in: int = DEFAULT_DEVICE_POLL_TIMEOUT
    interval: int = 5
    issued_at: float = field(default_factory=time.time)


class PhotonSetupError(RuntimeError):
    """初始化过程中的可读错误。"""


# ------------------------------------------------------------------ HTTP 基础
# 注意：必须在同一个 ClientSession 上下文内读取响应体，否则 session 关闭后
# 再读 body 会抛异常。因此这里统一 "请求+读 body" 一步完成。
async def _request_with_headers(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    json: dict | None = None,
    timeout: float = 30.0,
):
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        async with session.request(
            method, url, headers=headers, json=json
        ) as resp:
            status = resp.status
            resp_headers = resp.headers
            try:
                text = await resp.text()
            except Exception:  # noqa: BLE001
                text = ""
            return status, text, resp_headers


async def _request_text(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    json: dict | None = None,
    timeout: float = 30.0,
) -> tuple[int, str]:
    status, text, _ = await _request_with_headers(
        method, url, headers=headers, json=json, timeout=timeout
    )
    return status, text


def _parse_json(text: str, what: str):
    import json as _json

    try:
        return _json.loads(text or "{}")
    except Exception as e:  # noqa: BLE001
        raise PhotonSetupError(
            f"{what} 返回非 JSON: {e} body={text[:300]!r}"
        )


async def _request_json(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    body: dict | None = None,
    what: str = "请求",
    timeout: float = 30.0,
) -> tuple[int, dict | list]:
    status, text = await _request_text(
        method, url, headers=headers, json=body, timeout=timeout
    )
    parsed = _parse_json(text, what) if status >= 200 and status < 500 else None
    return status, parsed


async def _ensure_ok(
    status: int,
    parsed,
    what: str,
    *,
    ok_codes: tuple = (200,),
    auth_codes: tuple = (401, 403),
) -> None:
    if auth_codes and status in auth_codes:
        raise PhotonSetupError(f"{what} 鉴权失败 (HTTP {status})，可能使用了错误的凭据或登录态已失效。")
    if status not in ok_codes:
        detail = ""
        if isinstance(parsed, dict):
            detail = str(parsed.get("message") or parsed.get("error") or parsed.get("detail") or "")
        raise PhotonSetupError(f"{what}失败: HTTP {status} {detail}".strip())


async def _dashboard_get(path: str, token: str) -> dict | list:
    status, parsed = await _request_json(
        "GET", f"{DEFAULT_DASHBOARD_HOST}{path}",
        headers={"Authorization": f"Bearer {token}"},
        what="Dashboard GET " + path,
    )
    await _ensure_ok(status, parsed, "Dashboard GET " + path)
    return _unwrap_list(parsed) if not isinstance(parsed, list) else parsed


async def _dashboard_post(path: str, body: dict, token: str) -> dict:
    status, parsed = await _request_json(
        "POST", f"{DEFAULT_DASHBOARD_HOST}{path}", body=body,
        headers={"Authorization": f"Bearer {token}"},
        what="Dashboard POST " + path,
    )
    await _ensure_ok(status, parsed, "Dashboard POST " + path)
    data = parsed if isinstance(parsed, dict) else {}
    if data.get("error"):
        raise PhotonSetupError(f"Photon 操作失败: {data['error']}")
    if data.get("succeed") is False:
        raise PhotonSetupError(f"Photon 操作失败: {data.get('message') or data}")
    return data


async def _spectrum_request(
    method: str,
    path: str,
    project_id: str,
    project_secret: str | None,
    *,
    json_body: dict | None = None,
) -> dict | list:
    from base64 import b64encode

    auth = b64encode(f"{project_id}:{project_secret or ''}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    what = f"Spectrum {method} {path}"
    status, parsed = await _request_json(
        method, f"{DEFAULT_SPECTRUM_HOST}{path}",
        headers=headers, body=json_body, what=what,
    )
    await _ensure_ok(status, parsed, what)
    body = parsed if isinstance(parsed, dict) else {}
    data = body.get("data") if "data" in body else body
    if isinstance(body, dict) and body.get("error"):
        raise PhotonSetupError(f"Spectrum 操作失败: {body['error']}")
    return data


def _unwrap_list(data) -> list:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "projects", "users", "lines", "items"):
            inner = data.get(key)
            if isinstance(inner, list):
                return inner
            if isinstance(inner, dict):
                nested = next(
                    (
                        inner[k]
                        for k in ("projects", "users", "lines", "items")
                        if isinstance(inner.get(k), list)
                    ),
                    None,
                )
                if nested is not None:
                    return nested
    return []


def _normalize_phone(phone: str) -> str:
    return re.sub(r"[^\d+]", "", phone or "")


# ------------------------------------------------------------------ 设备登录
async def request_device_code() -> DeviceCode:
    """POST /api/auth/device/code，返回设备码 + 用户码。"""
    status, text = await _request_text(
        "POST",
        f"{DEFAULT_DASHBOARD_HOST}/api/auth/device/code",
        json={"client_id": DEFAULT_CLIENT_ID, "scope": DEFAULT_SCOPE},
    )
    await _ensure_ok(status, _parse_json(text, "设备登录"), "Photon 设备登录")
    data = _parse_json(text, "设备登录")
    return DeviceCode(
        device_code=data["device_code"],
        user_code=data["user_code"],
        verification_uri=data["verification_uri"],
        verification_uri_complete=data.get("verification_uri_complete"),
        expires_in=int(data.get("expires_in") or DEFAULT_DEVICE_POLL_TIMEOUT),
        interval=int(data.get("interval") or 5),
    )


async def poll_for_token(
    code: DeviceCode,
    *,
    interval: int | None = None,
    on_pending=None,
    timeout: int | None = None,
) -> str:
    """轮询 /api/auth/device/token 直到用户批准，返回 access_token。"""
    url = f"{DEFAULT_DASHBOARD_HOST}/api/auth/device/token"
    deadline = time.time() + (timeout or code.expires_in)
    sleep = interval or code.interval
    grant = {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": code.device_code,
        "client_id": DEFAULT_CLIENT_ID,
    }
    while time.time() < deadline:
        await asyncio.sleep(sleep)
        status, text, resp_headers = await _request_with_headers(
            "POST", url, json=grant
        )
        if status == 200:
            body = _parse_json(text, "设备登录")
            token = _extract_token_candidates(body) or _header_token(resp_headers)
            if token:
                return token[0]
            raise PhotonSetupError("Photon 返回 200 但未包含令牌。")
        if status == 429:
            sleep += 10
            if on_pending:
                on_pending()
            continue
        if status == 400:
            body = _parse_json(text, "设备登录")
            err = (body.get("error") or body.get("message") or "") if isinstance(body, dict) else ""
            if err in ("authorization_pending", "slow_down"):
                if err == "slow_down":
                    sleep += 5
                if on_pending:
                    on_pending()
                continue
            raise PhotonSetupError(f"Photon 登录失败: {err or 'authorization error'}")
        await asyncio.sleep(1)
    raise TimeoutError("Photon 登录已超时，请在浏览器中完成授权。")


def _extract_token_candidates(body) -> list[str]:
    body = body if isinstance(body, dict) else {}
    session = body.get("session") if isinstance(body.get("session"), dict) else {}
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    raw = (
        body.get("access_token"),
        body.get("accessToken"),
        session.get("access_token"),
        data.get("access_token"),
        data.get("accessToken"),
    )
    seen: set[str] = set()
    out: list[str] = []
    for value in raw:
        val = _clean_token(value)
        if val and val not in seen:
            seen.add(val)
            out.append(val)
    return out


def _header_token(resp_headers) -> list[str]:
    value = resp_headers.get("set-auth-token")
    cleaned = _clean_token(value)
    return [cleaned] if cleaned else []


def _clean_token(value) -> str | None:
    if not isinstance(value, str):
        return None
    token = value.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token or None


async def validate_photon_token(token: str) -> None:
    """校验 token 可访问项目 API。"""
    status, _ = await _request_text(
        "GET", f"{DEFAULT_DASHBOARD_HOST}/api/projects/",
        headers={"Authorization": f"Bearer {token}"},
    )
    if status in (401, 403):
        raise PhotonSetupError("Photon 令牌被项目 API 拒绝，请重新登录。")


# ------------------------------------------------------------------ 项目
async def list_projects(token: str) -> list[dict]:
    return await _dashboard_get("/api/projects", token)


async def find_project_by_name(token: str, name: str) -> dict | None:
    target = (name or "").strip().lower()
    for proj in await list_projects(token):
        if (proj.get("name") or "").strip().lower() == target:
            return proj
    return None


async def create_project(
    token: str, *, name: str = DEFAULT_PROJECT_NAME, location: str = "United States"
) -> dict:
    body = {
        "name": name,
        "location": location,
        "template": False,
        "observability": False,
    }
    data = await _dashboard_post("/api/projects", body, token)
    project = data.get("data") if isinstance(data.get("data"), dict) else data
    if not project.get("id"):
        raise PhotonSetupError("创建项目未返回 project id。")
    return project


async def regenerate_project_secret(token: str, project_id: str) -> str:
    data = await _dashboard_post(
        f"/api/projects/{project_id}/regenerate-secret", {}, token
    )
    secret = data.get("projectSecret")
    if not secret:
        raise PhotonSetupError("无法获取项目 secret。")
    return str(secret)


# ------------------------------------------------------------------ Spectrum 用户
async def list_users(project_id: str, project_secret: str) -> list[dict]:
    data = await _spectrum_request(
        "GET", f"/projects/{project_id}/users/", project_id, project_secret
    )
    return _unwrap_list(data) if not isinstance(data, list) else data


async def find_user_by_phone(
    project_id: str, project_secret: str, phone: str
) -> dict | None:
    target = _normalize_phone(phone)
    for user in await list_users(project_id, project_secret):
        # Spectrum 用户列表可能返回 {dict} 或裸字符串(电话号码)，做兼容
        if isinstance(user, dict):
            val = _normalize_phone(str(user.get("phoneNumber") or ""))
        elif isinstance(user, str):
            val = _normalize_phone(user)
        else:
            continue
        if val == target:
            return user if isinstance(user, dict) else {"phoneNumber": user}
    return None


async def register_user_if_absent(
    project_id: str,
    project_secret: str,
    *,
    phone: str,
    first_name: str | None = None,
    last_name: str | None = None,
    email: str | None = None,
) -> tuple[dict, bool]:
    if not E164_RE.match(phone):
        raise PhotonSetupError(f"phone 必须为 E.164 格式 (如 +15551234567)，收到: {phone!r}")
    existing = await find_user_by_phone(project_id, project_secret, phone)
    if existing:
        return existing, False
    body: dict = {"type": "shared", "phoneNumber": phone}
    if first_name:
        body["firstName"] = first_name
    if last_name:
        body["lastName"] = last_name
    if email:
        body["email"] = email
    user = await _spectrum_request(
        "POST", f"/projects/{project_id}/users/", project_id, project_secret,
        json_body=body,
    )
    # 兼容 create 返回不同形状：{user:{...}} / {data:{...}} / 直接是 dict / 是字符串 id
    if isinstance(user, dict):
        user = user.get("user") or user.get("data") or user
    elif isinstance(user, str):
        user = {"id": user, "phoneNumber": phone}
    return user, True


def user_assigned_line(user: dict | None) -> str | None:
    val = user.get("assignedPhoneNumber") if user else None
    return str(val) if val else None


# ------------------------------------------------------------------ iMessage line
async def list_lines(token: str, project_id: str) -> list[dict]:
    return await _dashboard_get(f"/api/projects/{project_id}/lines", token)


async def get_imessage_line(
    token: str, project_id: str, *, create_if_missing: bool = True
) -> dict | None:
    for line in await list_lines(token, project_id):
        if (line.get("platform") or "").lower() == "imessage":
            return line
    if not create_if_missing:
        return None
    data = await _dashboard_post(
        f"/api/projects/{project_id}/lines", {"platform": "imessage"}, token
    )
    return data.get("line") or data