from __future__ import annotations

from urllib.parse import parse_qs

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse


router = APIRouter()


def _templates(request: Request):
    return request.app.state.templates


def _repo(request: Request):
    return request.app.state.web_repo


def _auth_service(request: Request):
    return request.app.state.auth_service


def _current_user(request: Request):
    return _auth_service(request).get_current_user(request)


def _page_context(request: Request, **extra):
    current_user = _current_user(request)
    return {
        "request": request,
        "current_user": current_user,
        "is_admin": _auth_service(request).is_admin(current_user),
        **extra,
    }


def _home_redirect(request: Request) -> RedirectResponse:
    return RedirectResponse(url=f"{request.app.state.web_config.root_path}/", status_code=303)


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    repo = _repo(request)
    strategies = repo.list_strategies()
    runs = repo.list_runs(limit=10)
    top_cagr = sorted(strategies, key=lambda item: item.metrics.get("cagr", 0.0), reverse=True)[:5]
    top_sharpe = sorted(
        strategies, key=lambda item: item.metrics.get("sharpe", 0.0), reverse=True
    )[:5]
    low_dd = sorted(strategies, key=lambda item: item.metrics.get("max_drawdown", 0.0), reverse=True)[:5]
    return _templates(request).TemplateResponse(
        "dashboard.html",
        _page_context(
            request,
            strategies=strategies,
            runs=runs,
            top_cagr=top_cagr,
            top_sharpe=top_sharpe,
            low_dd=low_dd,
            last_refresh_at=repo.get_setting("last_snapshot_refresh_at"),
        ),
    )


@router.get("/strategies", response_class=HTMLResponse)
def strategies_page(request: Request):
    repo = _repo(request)
    return _templates(request).TemplateResponse(
        "strategies.html",
        _page_context(request, strategies=repo.list_strategies()),
    )


@router.get("/strategies/{strategy_key}", response_class=HTMLResponse)
def strategy_detail_page(request: Request, strategy_key: str):
    repo = _repo(request)
    strategy = repo.get_strategy_latest(strategy_key)
    definition = repo.get_strategy_definition(strategy_key)
    latest_run_id = strategy.latest_run_id if strategy else None
    if definition is None:
        return _templates(request).TemplateResponse(
            "strategy_detail.html",
            _page_context(request, strategy=None, definition=None, run_id=None),
            status_code=404,
        )
    return _templates(request).TemplateResponse(
        "strategy_detail.html",
        _page_context(request, strategy=strategy, definition=definition, run_id=latest_run_id),
    )


@router.get("/composer", response_class=HTMLResponse)
def composer_page(request: Request):
    repo = _repo(request)
    return _templates(request).TemplateResponse(
        "composer.html",
        _page_context(
            request,
            strategies=[s for s in repo.list_strategies() if s.latest_run_id and s.supports_composer],
        ),
    )


@router.get("/runs", response_class=HTMLResponse)
def runs_page(request: Request):
    repo = _repo(request)
    return _templates(request).TemplateResponse(
        "runs.html",
        _page_context(request, runs=repo.list_runs(limit=100)),
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _current_user(request) is not None:
        return _home_redirect(request)
    return _templates(request).TemplateResponse(
        "login.html",
        _page_context(request, error=None, attempted_username=""),
    )


@router.post("/login")
async def login_submit(request: Request):
    body = await request.body()
    form = {key: values[0] for key, values in parse_qs(body.decode("utf-8")).items()}
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))
    user = _auth_service(request).authenticate(username, password)
    if user is None:
        return _templates(request).TemplateResponse(
            "login.html",
            _page_context(request, error="用户名或密码错误", attempted_username=username),
            status_code=401,
        )
    _auth_service(request).login(request, user)
    return _home_redirect(request)


@router.post("/logout")
def logout(request: Request):
    _auth_service(request).logout(request)
    return _home_redirect(request)


@router.get("/healthz")
def healthz():
    return {"status": "ok"}
