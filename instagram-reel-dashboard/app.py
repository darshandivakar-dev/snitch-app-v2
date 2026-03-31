"""
Snitch Instagram Reel Analytics — Backend v7.0
Production-ready FastAPI server for cloud deployment (Railway / Render / Fly.io)
"""
from __future__ import annotations
import re, asyncio, json, os
from typing import Optional, Dict, List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="Snitch Reel Analytics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Serve frontend ─────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return HTMLResponse("<h1>Snitch API running</h1><p>Place index.html in the same directory.</p>")

# ── Shortcode extraction ───────────────────────────────────────────────────────
_SC_RE = re.compile(r"instagram\.com/(?:reel|p|tv)/([A-Za-z0-9_-]+)")

def _extract_sc(url: str) -> str:
    url = url.strip().rstrip("/")
    m = _SC_RE.search(url)
    if m:
        return m.group(1)
    parts = [p for p in url.split("/") if p]
    return parts[-1]

# ── View-count key priority ────────────────────────────────────────────────────
_VIEW_KEYS = (
    "play_count",
    "clips_aggregated_view_count",
    "ig_play_count",
    "video_play_count",
    "video_view_count",
    "view_count",
)

def _max_from_node(obj, depth: int = 0) -> Optional[int]:
    best: Optional[int] = None
    def _walk(o, d):
        nonlocal best
        if d > 14 or not isinstance(o, (dict, list)):
            return
        if isinstance(o, dict):
            for k in _VIEW_KEYS:
                v = o.get(k)
                if isinstance(v, (int, float)) and int(v) > 0:
                    best = max(best, int(v)) if best is not None else int(v)
            for v in o.values():
                if isinstance(v, (dict, list)):
                    _walk(v, d + 1)
        elif isinstance(o, list):
            for item in o:
                _walk(item, d + 1)
    _walk(obj, depth)
    return best

def _safe_int(val) -> Optional[int]:
    try:
        v = int(val)
        return v if v >= 0 else None
    except (TypeError, ValueError):
        return None

# ── Profile cache ──────────────────────────────────────────────────────────────
_profile_cache: Dict[str, dict] = {}

def _get_profile(L, username: str) -> dict:
    if username in _profile_cache:
        return _profile_cache[username]
    try:
        import instaloader
        prof = instaloader.Profile.from_username(L.context, username)
        data = {"followers": prof.followers, "is_verified": prof.is_verified}
    except Exception:
        data = {"followers": None, "is_verified": False}
    _profile_cache[username] = data
    return data

# ── View extractor ─────────────────────────────────────────────────────────────
def _get_views(post) -> Optional[int]:
    try:
        best = _max_from_node(post._node)
        if best is not None:
            return best
    except Exception:
        pass
    for attr in ("video_play_count", "video_view_count", "play_count"):
        try:
            v = getattr(post, attr, None)
            if v is not None and int(v) > 0:
                return int(v)
        except Exception:
            pass
    return None

# ── instaloader scraper ────────────────────────────────────────────────────────
def _scrape_with_loader(shortcodes: List[str], username: str = None, password: str = None) -> dict:
    try:
        import instaloader
    except ImportError:
        return {sc: {"error": "instaloader not installed"} for sc in shortcodes}

    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
    )

    if username and password:
        try:
            L.login(username, password)
        except Exception as e:
            print(f"[login] failed: {e}")
    else:
        # Try loading a saved session file if it exists
        session_file = os.path.expanduser("~/.instaloader-session")
        try:
            if os.path.exists(session_file):
                L.load_session_from_file(open(session_file).read().strip(), session_file)
        except Exception:
            pass

    results = {}
    for sc in shortcodes:
        try:
            post = instaloader.Post.from_shortcode(L.context, sc)

            # Views — walk raw node + top-up via /?__a=1
            views = _get_views(post)
            try:
                a1 = L.context.get_json(f"p/{sc}/", params={"__a": "1", "__d": "dis"})
                pc = _max_from_node(a1)
                if pc and (views is None or pc > views):
                    views = pc
            except Exception:
                pass

            # Likes / comments
            likes = None
            try:
                likes = _safe_int(post.likes)
            except Exception:
                pass

            comments = None
            try:
                comments = _safe_int(post.comments)
            except Exception:
                pass

            # Author
            owner = "unknown"
            try:
                owner = post.owner_username
            except Exception:
                try:
                    owner = post.owner_profile.username
                except Exception:
                    pass

            # Profile
            prof = _get_profile(L, owner)
            followers = prof.get("followers")
            is_verified = prof.get("is_verified", False)

            # Post date
            post_date = None
            try:
                post_date = post.date_utc.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass

            # Duration
            duration = None
            try:
                d = post.video_duration
                if d is not None:
                    duration = round(float(d), 1)
            except Exception:
                pass

            # Hashtags / mentions
            hashtags = []
            try:
                hashtags = list(post.caption_hashtags)[:15]
            except Exception:
                pass

            # Thumbnail
            thumbnail = None
            try:
                thumbnail = post.url
            except Exception:
                pass

            # Caption
            caption = ""
            try:
                caption = (post.caption or "")[:200]
            except Exception:
                pass

            # Derived
            er = None
            if views and views > 0 and likes is not None and comments is not None:
                er = round((likes + comments) / views * 100, 2)

            view_rate = None
            if views and followers and followers > 0:
                view_rate = round(views / followers * 100, 2)

            results[sc] = {
                "views": views, "likes": likes, "comments": comments,
                "engagement_rate": er, "view_rate": view_rate,
                "author": owner, "handle": f"@{owner}",
                "is_verified": is_verified, "followers": followers,
                "post_date": post_date, "duration": duration,
                "hashtags": hashtags, "thumbnail": thumbnail,
                "caption": caption, "source": "instaloader",
            }
        except Exception as e:
            results[sc] = {"error": str(e), "source": "instaloader"}

    return results

# ── HTML-based fallback scraper ───────────────────────────────────────────────
_VIEW_PATTERNS   = [re.compile(r'"' + k + r'"\s*:\s*(\d+)') for k in _VIEW_KEYS]
_LIKES_PATTERN   = re.compile(r'"edge_media_preview_like"\s*:\s*\{"count"\s*:\s*(\d+)')
_COMMENT_PATTERN = re.compile(r'"edge_media_to_comment"\s*:\s*\{"count"\s*:\s*(\d+)')
_FOLLOW_PATTERN  = re.compile(r'"edge_followed_by"\s*:\s*\{"count"\s*:\s*(\d+)')
_OWNER_PATTERN   = re.compile(r'"username"\s*:\s*"([^"]+)"')
_DATE_PATTERN    = re.compile(r'"taken_at_timestamp"\s*:\s*(\d+)')
_DURATION_PATTERN= re.compile(r'"video_duration"\s*:\s*([\d.]+)')
_HASHTAG_PATTERN = re.compile(r'#(\w+)')

def _parse_html(html: str) -> dict:
    views = None
    for pat in _VIEW_PATTERNS:
        for m in pat.finditer(html):
            v = int(m.group(1))
            if v > 0:
                views = max(views, v) if views else v

    likes    = None
    m = _LIKES_PATTERN.search(html)
    if m: likes = _safe_int(m.group(1))

    comments = None
    m = _COMMENT_PATTERN.search(html)
    if m: comments = _safe_int(m.group(1))

    followers = None
    m = _FOLLOW_PATTERN.search(html)
    if m: followers = _safe_int(m.group(1))

    owner = None
    m = _OWNER_PATTERN.search(html)
    if m: owner = m.group(1)

    post_date = None
    m = _DATE_PATTERN.search(html)
    if m:
        import datetime
        post_date = datetime.datetime.utcfromtimestamp(int(m.group(1))).strftime("%Y-%m-%d %H:%M")

    duration = None
    m = _DURATION_PATTERN.search(html)
    if m: duration = round(float(m.group(1)), 1)

    hashtags = list(dict.fromkeys(_HASHTAG_PATTERN.findall(html)))[:15]

    er = None
    if views and views > 0 and likes is not None and comments is not None:
        er = round((likes + comments) / views * 100, 2)
    view_rate = None
    if views and followers and followers > 0:
        view_rate = round(views / followers * 100, 2)

    return {
        "views": views, "likes": likes, "comments": comments,
        "followers": followers, "author": owner,
        "handle": f"@{owner}" if owner else None,
        "post_date": post_date, "duration": duration,
        "hashtags": hashtags, "engagement_rate": er,
        "view_rate": view_rate, "is_verified": False,
        "source": "html_fallback",
    }

async def _scrape_playwright(shortcodes: List[str]) -> dict:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return {sc: {"error": "playwright not available on this server"} for sc in shortcodes}

    results = {}
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
            ])
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
                ),
                viewport={"width": 390, "height": 844},
            )
            page = await ctx.new_page()
            await page.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            for sc in shortcodes:
                try:
                    await page.goto(f"https://www.instagram.com/reel/{sc}/",
                                    wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(2)
                    html = await page.content()
                    data = _parse_html(html)
                    # Also scan script tags for JSON
                    scripts = await page.query_selector_all("script[type='application/json']")
                    for s in scripts:
                        try:
                            obj = json.loads(await s.inner_text())
                            v = _max_from_node(obj)
                            if v and (data["views"] is None or v > data["views"]):
                                data["views"] = v
                        except Exception:
                            pass
                    if data["views"] and data["likes"] is not None and data["comments"] is not None:
                        data["engagement_rate"] = round(
                            (data["likes"] + data["comments"]) / data["views"] * 100, 2
                        )
                    if data["views"] and data["followers"]:
                        data["view_rate"] = round(data["views"] / data["followers"] * 100, 2)
                    results[sc] = data
                except Exception as e:
                    results[sc] = {"error": str(e), "source": "playwright"}
            await browser.close()
    except Exception as e:
        for sc in shortcodes:
            if sc not in results:
                results[sc] = {"error": str(e), "source": "playwright_init"}
    return results

# ── API ────────────────────────────────────────────────────────────────────────
class ScrapeRequest(BaseModel):
    urls: List[str]
    username: Optional[str] = None
    password: Optional[str] = None

@app.post("/api/scrape")
async def scrape(req: ScrapeRequest):
    shortcodes = [_extract_sc(u) for u in req.urls if u.strip()]
    if not shortcodes:
        raise HTTPException(400, "No valid URLs provided")

    loop = asyncio.get_event_loop()
    il_results = await loop.run_in_executor(
        None, _scrape_with_loader, shortcodes, req.username, req.password
    )

    # Use Playwright for any that failed or have no views
    need_pw = [
        sc for sc in shortcodes
        if "error" in il_results.get(sc, {}) or il_results.get(sc, {}).get("views") is None
    ]
    pw_results = {}
    if need_pw:
        pw_results = await _scrape_playwright(need_pw)

    # Merge results
    final = {}
    for sc in shortcodes:
        il = il_results.get(sc, {})
        pw = pw_results.get(sc, {})
        if "error" in il and "error" not in pw:
            final[sc] = pw
        elif "error" not in il:
            merged = dict(il)
            for k, v in pw.items():
                if merged.get(k) is None and v is not None:
                    merged[k] = v
            views = merged.get("views")
            likes = merged.get("likes")
            comments = merged.get("comments")
            followers = merged.get("followers")
            if views and views > 0 and likes is not None and comments is not None:
                merged["engagement_rate"] = round((likes + comments) / views * 100, 2)
            if views and followers and followers > 0:
                merged["view_rate"] = round(views / followers * 100, 2)
            final[sc] = merged
        else:
            final[sc] = {"shortcode": sc, "error": "Both scrapers failed"}

    return {"results": final}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "snitch-reel-analytics"}

@app.get("/api/debug/{shortcode}")
async def debug(shortcode: str):
    try:
        import instaloader
        L = instaloader.Instaloader(quiet=True)
        post = instaloader.Post.from_shortcode(L.context, shortcode)
        node = post._node
        return {
            "shortcode": shortcode,
            "view_candidates": {k: node.get(k) for k in _VIEW_KEYS if k in node},
            "node_keys": list(node.keys())[:50],
            "views_result": _get_views(post),
        }
    except Exception as e:
        return {"error": str(e)}

# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
