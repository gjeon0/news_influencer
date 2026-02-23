# -*- coding: utf-8 -*-
"""
API.py - TikTok Hidden API Endpoints with CSV Export
=====================================================
Module for accessing TikTok's internal JSON endpoints and exporting to CSV.

All endpoints now support CSV export with all available fields.
"""

from __future__ import annotations

import os
import time
import json
import re
import random
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, quote
from datetime import datetime

import pandas as pd
import undetected_chromedriver as uc
import browser_cookie3

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("API")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TIKTOK_BASE = "https://www.tiktok.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

URL_REGEX = r"(?<=\.com/)(.+?)(?=\?|$)"
VIDEO_ID_REGEX = r"(?<=/video/)([0-9]+)"

# Hidden-API endpoint map
HIDDEN_API_ENDPOINTS = {
    "user_detail":        "/api/user/detail/",
    "user_videos":        "/api/post/item_list/",
    "user_liked":         "/api/favorite/item_list/",
    "user_playlists":     "/api/user/playlist",
    "hashtag_detail":     "/api/challenge/detail/",
    "hashtag_videos":     "/api/challenge/item_list/",
    "video_comments":     "/api/comment/list/",
    "comment_replies":    "/api/comment/list/reply/",
    "related_videos":     "/api/related/item_list/",
    "trending":           "/api/recommend/item_list/",
    "search_users":       "/api/search/user/full/",
    "search_items":       "/api/search/item/full/",
    "search_general":     "/api/search/general/full/",
    "sound_detail":       "/api/music/detail/",
    "sound_videos":       "/api/music/item_list/",
    "playlist_detail":    "/api/mix/detail/",
    "playlist_videos":    "/api/mix/item_list/",
}

# Stealth script
_STEALTH_SCRIPT = """
(function(){
    delete Object.getPrototypeOf(navigator).webdriver;
    if(!window.chrome){window.chrome={};}
    if(!window.chrome.runtime){
        window.chrome.runtime={id:undefined,connect:null,sendMessage:null};
    }
    var nd = Object.getPrototypeOf(navigator);
    Object.defineProperty(nd,'languages',{get:()=>['en-US','en']});
    Object.defineProperty(nd,'platform',{get:()=>'MacIntel'});
    Object.defineProperty(nd,'vendor',{get:()=>'Google Inc.'});
    var gp = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(p){
        if(p===37445) return 'Intel Inc.';
        if(p===37446) return 'Intel Iris OpenGL Engine';
        return gp.apply(this,arguments);
    };
})();
"""


def _safe(obj, *keys, default=""):
    """Drill into nested dicts safely."""
    for k in keys:
        if isinstance(obj, dict):
            obj = obj.get(k)
        else:
            return default
    return obj if obj is not None else default


def generate_video_data_row(video_obj: Dict[str, Any]) -> pd.DataFrame:
    """Convert a TikTok video-object dict into a single-row DataFrame with all fields."""
    # timestamp
    try:
        ts = datetime.fromtimestamp(int(video_obj["createTime"])).isoformat()
    except Exception:
        ts = ""

    # stickers
    try:
        stickers = ";".join(
            t for s in video_obj.get("stickersOnItem", []) for t in s.get("stickerText", [])
        )
    except Exception:
        stickers = ""

    row = {
        "video_id":                 video_obj.get("id", ""),
        "video_timestamp":          ts,
        "video_duration":           _safe(video_obj, "video", "duration", default=""),
        "video_locationcreated":    video_obj.get("locationCreated", ""),
        "video_diggcount":          _safe(video_obj, "stats", "diggCount", default=""),
        "video_sharecount":         _safe(video_obj, "stats", "shareCount", default=""),
        "video_commentcount":       _safe(video_obj, "stats", "commentCount", default=""),
        "video_playcount":          _safe(video_obj, "stats", "playCount", default=""),
        "video_description":        video_obj.get("desc", ""),
        "video_is_ad":              video_obj.get("isAd", False),
        "video_stickers":           stickers,
        "author_username":          _safe(video_obj, "author", "uniqueId", default=""),
        "author_name":              _safe(video_obj, "author", "nickname", default=""),
        "author_followercount":     _safe(video_obj, "authorStats", "followerCount", default=""),
        "author_followingcount":    _safe(video_obj, "authorStats", "followingCount", default=""),
        "author_heartcount":        _safe(video_obj, "authorStats", "heartCount", default=""),
        "author_videocount":        _safe(video_obj, "authorStats", "videoCount", default=""),
        "author_diggcount":         _safe(video_obj, "authorStats", "diggCount", default=""),
        "author_verified":          _safe(video_obj, "author", "verified", default=False),
        "poi_name":                 _safe(video_obj, "poi", "name", default=""),
        "poi_address":              _safe(video_obj, "poi", "address", default=""),
        "poi_city":                 _safe(video_obj, "poi", "city", default=""),
    }
    return pd.DataFrame([row])


def _comment_export_row(comment_obj: Dict[str, Any], aweme_id: str = "") -> Dict[str, Any]:
    """Normalize a raw comment object to a rich export row."""
    user_obj = comment_obj.get("user", {}) if isinstance(comment_obj, dict) else {}
    return {
        "allow_download_photo":    comment_obj.get("allow_download_photo", False),
        "author_pin":              comment_obj.get("author_pin", False),
        "aweme_id":                str(comment_obj.get("aweme_id", aweme_id or "")),
        "cid":                     str(comment_obj.get("cid", "")),
        "collect_stat":            comment_obj.get("collect_stat", 0),
        "comment_language":        comment_obj.get("comment_language", ""),
        "comment_post_item_ids":   comment_obj.get("comment_post_item_ids", ""),
        "create_time":             comment_obj.get("create_time", 0),
        "digg_count":              comment_obj.get("digg_count", 0),
        "image_list":              comment_obj.get("image_list", ""),
        "is_author_digged":        comment_obj.get("is_author_digged", False),
        "is_comment_translatable": comment_obj.get("is_comment_translatable", False),
        "is_high_purchase_intent": comment_obj.get("is_high_purchase_intent", False),
        "label_list":              comment_obj.get("label_list", ""),
        "no_show":                 comment_obj.get("no_show", False),
        "reply_comment":           comment_obj.get("reply_comment", ""),
        "reply_comment_total":     comment_obj.get("reply_comment_total", 0),
        "reply_id":                comment_obj.get("reply_id", 0),
        "reply_to_reply_id":       comment_obj.get("reply_to_reply_id", 0),
        "share_info":              comment_obj.get("share_info", ""),
        "sort_extra_score":        comment_obj.get("sort_extra_score", ""),
        "sort_tags":               comment_obj.get("sort_tags", ""),
        "status":                  comment_obj.get("status", 0),
        "stick_position":          comment_obj.get("stick_position", 0),
        "text":                    comment_obj.get("text", ""),
        "text_extra":              comment_obj.get("text_extra", ""),
        "trans_btn_style":         comment_obj.get("trans_btn_style", 0),
        "user":                    comment_obj.get("user", ""),
        "user_buried":             comment_obj.get("user_buried", False),
        "user_digged":             comment_obj.get("user_digged", 0),
        "user_unique_id":          user_obj.get("unique_id", ""),
        "user_nickname":           user_obj.get("nickname", ""),
        "user_uid":                user_obj.get("uid", ""),
    }


def _deduplicate(existing_path: str, new_df: pd.DataFrame, key: str = "video_id") -> pd.DataFrame:
    """Merge new_df with an existing CSV (if present) and drop duplicates on key."""
    if os.path.exists(existing_path):
        old = pd.read_csv(existing_path, keep_default_na=False)
        new_df = pd.concat([old, new_df], ignore_index=True)
    new_df[key] = new_df[key].astype(str)
    return new_df.drop_duplicates(subset=[key])


def _sanitize_filename_part(s: str) -> str:
    s = str(s or "").strip()
    if not s:
        return "unknown"
    s = s.replace("/", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_@\-\.]+", "", s)
    return s or "unknown"


def _dom_style_base_name_from_url(source_url: str) -> str:
    regex_url = re.findall(URL_REGEX, str(source_url or ""))
    base_name = regex_url[0].replace("/", "_") if regex_url else str(source_url or "").replace("/", "_")
    return base_name or "output"


def _default_csv_filename(endpoint: str, identifier: str = "") -> str:
    """Match the DOM module's style of filenames where possible.

    - user pages:            https://www.tiktok.com/@user     -> @user.csv
    - hashtag pages:         https://www.tiktok.com/tag/tag   -> tag_tag.csv
    - related page (URL):    https://www.tiktok.com/@u/video/id -> @u_video_id.csv
    - comments:              comments_<video_id>.csv
    For non-page endpoints (search/sound/playlist), we synthesize a URL-like
    path so the naming is stable and consistent.
    """
    endpoint = (endpoint or "").strip().lower()
    identifier = str(identifier or "").strip()

    if endpoint == "user_videos":
        url = f"{TIKTOK_BASE}/@{identifier.lstrip('@').strip()}"
        return f"{_dom_style_base_name_from_url(url)}.csv"
    if endpoint == "user_info":
        url = f"{TIKTOK_BASE}/@{identifier.lstrip('@').strip()}"
        return f"{_dom_style_base_name_from_url(url)}_user_info.csv"
    if endpoint == "hashtag_videos":
        url = f"{TIKTOK_BASE}/tag/{identifier.lstrip('#').strip()}"
        return f"{_dom_style_base_name_from_url(url)}.csv"
    if endpoint == "video_comments":
        m = re.search(VIDEO_ID_REGEX, identifier)
        vid = m.group(1) if m else _sanitize_filename_part(identifier)
        return f"comments_{vid}.csv"
    if endpoint == "related_videos":
        if "tiktok.com" in identifier:
            return f"{_dom_style_base_name_from_url(identifier)}.csv"
        m = re.search(VIDEO_ID_REGEX, identifier)
        vid = m.group(1) if m else _sanitize_filename_part(identifier)
        return f"related_{vid}.csv"
    if endpoint == "trending":
        url = f"{TIKTOK_BASE}/foryou"
        return f"{_dom_style_base_name_from_url(url)}.csv"
    if endpoint == "search_videos":
        kw = _sanitize_filename_part(identifier)
        url = f"{TIKTOK_BASE}/search/{kw}"
        return f"{_dom_style_base_name_from_url(url)}.csv"
    if endpoint == "search_users":
        kw = _sanitize_filename_part(identifier)
        url = f"{TIKTOK_BASE}/search/user/{kw}"
        return f"{_dom_style_base_name_from_url(url)}.csv"
    if endpoint == "sound_videos":
        sid = _sanitize_filename_part(identifier)
        url = f"{TIKTOK_BASE}/music/{sid}"
        return f"{_dom_style_base_name_from_url(url)}.csv"
    if endpoint == "sound_info":
        sid = _sanitize_filename_part(identifier)
        url = f"{TIKTOK_BASE}/music/{sid}"
        return f"{_dom_style_base_name_from_url(url)}_info.csv"
    if endpoint == "playlist_videos":
        pid = _sanitize_filename_part(identifier)
        url = f"{TIKTOK_BASE}/playlist/{pid}"
        return f"{_dom_style_base_name_from_url(url)}.csv"
    if endpoint == "playlist_info":
        pid = _sanitize_filename_part(identifier)
        url = f"{TIKTOK_BASE}/playlist/{pid}"
        return f"{_dom_style_base_name_from_url(url)}_info.csv"

    # Fallback
    safe_id = _sanitize_filename_part(identifier) if identifier else endpoint
    return f"{endpoint}_{safe_id}.csv"


# ===========================================================================
# SeleniumDriverAPI - for hidden API calls
# ===========================================================================
class SeleniumDriverAPI:
    """Selenium driver for TikTok hidden API calls with X-Bogus signing."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver: Optional[uc.Chrome] = None
        self._hidden_api_session_params: Optional[Dict[str, Any]] = None

    def start(self, cookies_jar=None):
        """Start ChromeDriver with retry logic."""
        max_attempts = 3
        last_error = None

        for attempt in range(1, max_attempts + 1):
            try:
                opts = uc.ChromeOptions()
                if self.headless:
                    opts.add_argument("--headless=new")
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")
                opts.add_argument("--disable-blink-features=AutomationControlled")
                opts.add_argument("--window-size=1920,1080")
                opts.add_argument(f"--user-agent={HEADERS['User-Agent']}")

                prefs: Dict[str, Any] = {
                    "credentials_enable_service": False,
                    "profile.password_manager_enabled": False,
                }
                opts.add_experimental_option("prefs", prefs)

                self.driver = uc.Chrome(options=opts, use_subprocess=False, version_main=144)
                time.sleep(0.5)

                # Test if driver is working
                self.driver.get("about:blank")
                logger.info(f"ChromeDriver started successfully (attempt {attempt}/{max_attempts})")
                break

            except Exception as e:
                last_error = e
                logger.warning(f"ChromeDriver start failed (attempt {attempt}/{max_attempts}): {e}")

                # Clean up failed driver
                if self.driver:
                    try:
                        self.driver.quit()
                    except:
                        pass
                    self.driver = None

                if attempt < max_attempts:
                    wait_time = attempt * 2
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)

        if self.driver is None:
            raise RuntimeError(f"Failed to start ChromeDriver after {max_attempts} attempts: {last_error}")

        # Set script timeout for async operations (60 seconds)
        self.driver.set_script_timeout(60)

        # Inject stealth JS
        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument", {"source": _STEALTH_SCRIPT}
            )
        except Exception:
            pass

        # Load TikTok page and warm up session
        logger.info("Loading TikTok and warming up session...")
        self.driver.get(TIKTOK_BASE)
        time.sleep(2)

        if cookies_jar:
            self._inject_cookies(cookies_jar)

        # Warm up by visiting a few pages to establish session
        try:
            logger.info("Warming up TikTok session...")
            self.driver.get(TIKTOK_BASE + "/foryou")
            time.sleep(2)
            self.driver.get(TIKTOK_BASE + "/@tiktok")
            time.sleep(1.5)
            logger.info("Session warmup complete")
        except Exception as e:
            logger.warning(f"Session warmup had issues (continuing anyway): {e}")

    def stop(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop()

    def _inject_cookies(self, cookies_jar):
        """Push browser_cookie3 jar into Selenium session."""
        for c in cookies_jar:
            try:
                d: Dict[str, Any] = {"name": c.name, "value": c.value}
                if c.domain and "tiktok.com" in c.domain:
                    d["domain"] = c.domain
                if c.path:
                    d["path"] = c.path
                if c.expires:
                    d["expiry"] = int(c.expires)
                if c.secure is not None:
                    d["secure"] = c.secure
                self.driver.add_cookie(d)
            except Exception:
                continue
        self.driver.refresh()
        time.sleep(1.5)

    def get_ms_token(self) -> Optional[str]:
        """Read msToken from current page cookies."""
        for c in self.driver.get_cookies():
            if c.get("name") == "msToken":
                return c.get("value")
        return None

    def go(self, url: str, wait: float = 2.0):
        """Navigate to URL with small random jitter."""
        self.driver.get(url)
        time.sleep(wait + random.uniform(0.0, 0.8))

    def _ensure_hidden_api_session_params(self) -> Dict[str, Any]:
        """Build and cache common hidden-API query parameters.

        TikTok's internal endpoints are sensitive to missing/odd client hints.
        A minimal (aid/app_name/device_id only) param set often works for some
        endpoints but fails or returns empty bodies for others.

        This mirrors the richer parameter set used in
        folder/test_lab/unified_pyktok.py.
        """
        if self._hidden_api_session_params is not None:
            return self._hidden_api_session_params

        def _js(expr: str, default: str = "") -> str:
            try:
                val = self.driver.execute_script(f"return ({expr});")
                return str(val) if val is not None else default
            except Exception:
                return default

        user_agent = _js("navigator.userAgent", HEADERS.get("User-Agent", ""))
        language = _js("navigator.language || navigator.userLanguage", "en")
        platform = _js("navigator.platform", "MacIntel")
        tz_name = _js("Intl.DateTimeFormat().resolvedOptions().timeZone", "America/New_York")
        screen_height = _js("window.screen && window.screen.height", "1080")
        screen_width = _js("window.screen && window.screen.width", "1920")

        self._hidden_api_session_params = {
            "aid": "1988",
            "app_language": language,
            "app_name": "tiktok_web",
            "browser_language": language,
            "browser_name": "Mozilla",
            "browser_online": "true",
            "browser_platform": platform,
            "browser_version": user_agent,
            "channel": "tiktok_web",
            "cookie_enabled": "true",
            "device_id": str(random.randint(10**18, 10**19 - 1)),
            "device_platform": "web_pc",
            "focus_state": "true",
            "from_page": "user",
            "history_len": str(random.randint(1, 10)),
            "is_fullscreen": "false",
            "is_page_visible": "true",
            "language": language,
            "os": platform,
            "priority_region": "",
            "referer": "",
            "region": "US",
            "screen_height": screen_height,
            "screen_width": screen_width,
            "tz_name": tz_name,
            "webcast_language": language,
        }
        return self._hidden_api_session_params

    def fetch_api(self, endpoint_key: str, params: Dict[str, Any]) -> Optional[Dict]:
        """
        Sign params and fetch from TikTok hidden API endpoint.
        Returns parsed JSON response or None on failure.
        """
        path = HIDDEN_API_ENDPOINTS[endpoint_key]
        base_url = TIKTOK_BASE + path

        # Ensure base params
        base_params = self._ensure_hidden_api_session_params()

        # Wait for byted_acrawler
        max_sign_attempts = 5
        acrawler_ready = False
        try_urls = [
            TIKTOK_BASE + "/foryou",
            TIKTOK_BASE,
            TIKTOK_BASE + "/@tiktok",
        ]
        for attempt in range(max_sign_attempts):
            try:
                result = self.driver.execute_script(
                    "return (typeof window.byted_acrawler !== 'undefined') ? true : false;"
                )
                if result:
                    acrawler_ready = True
                    break
            except Exception:
                pass
            self.go(random.choice(try_urls), wait=2.0)
            time.sleep(random.uniform(2, 4))

        if not acrawler_ready:
            logger.error("byted_acrawler never loaded after %d attempts", max_sign_attempts)
            return None

        # Fetch JS
        js = """
        var done = arguments[arguments.length - 1];
        var url  = arguments[0];
        fetch(url, {
            method: 'GET',
            credentials: 'same-origin'
        })
        .then(function(r) {
            return r.text().then(function(text) {
                done('__STATUS_' + r.status + '__' + text);
            });
        })
        .catch(function(err) { done('__FETCH_ERROR__:' + err.message); });
        """

        def _expected_keys(key: str) -> List[str]:
            if key in {"user_detail"}:
                return ["userInfo"]
            if key in {"hashtag_detail"}:
                return ["challengeInfo"]
            if key in {"video_comments"}:
                return ["comments"]
            if key in {"search_users"}:
                return ["user_list"]
            if key in {"search_items"}:
                return ["item_list", "itemList", "data"]
            if key in {"search_general"}:
                return ["data", "item_list", "itemList"]
            if key in {"sound_detail"}:
                return ["musicInfo", "music"]
            if key in {"playlist_detail"}:
                return ["mixInfo", "mix"]
            return ["itemList"]

        def _do_fetch(signed_url: str) -> Tuple[Optional[str], str]:
            """Return (http_status, body_text)."""
            self.driver.set_script_timeout(30)
            raw = self.driver.execute_async_script(js, signed_url)
            if isinstance(raw, str) and raw.startswith("__FETCH_ERROR__:"):
                return None, ""
            if isinstance(raw, str) and raw.startswith("__STATUS_"):
                status_end = raw.index("__", 9)
                status_code = raw[9:status_end]
                body = raw[status_end + 2:]
                return status_code, body
            return None, str(raw) if raw is not None else ""

        max_retries = 5
        sleep_range = (2.0, 4.0)

        for attempt in range(1, max_retries + 1):
            try:
                # Refresh msToken each attempt
                ms_token = self.get_ms_token() or ""

                merged_params: Dict[str, Any] = {**base_params, **params, "msToken": ms_token}
                if endpoint_key == "trending":
                    merged_params.setdefault("from_page", "fyp")
                elif endpoint_key == "hashtag_videos":
                    merged_params.setdefault("from_page", "challenge")
                elif endpoint_key in {"related_videos", "video_comments"}:
                    merged_params.setdefault("from_page", "video")

                query = urlencode(merged_params, safe="=", quote_via=quote)
                full_url = f"{base_url}?{query}"

                # Generate X-Bogus
                try:
                    sign_result = self.driver.execute_script(
                        "return window.byted_acrawler.frontierSign(arguments[0]);",
                        full_url,
                    )
                    x_bogus = sign_result.get("X-Bogus", "") if isinstance(sign_result, dict) else ""
                except Exception as exc:
                    logger.warning("X-Bogus generation failed: %s", exc)
                    x_bogus = ""

                signed_url = full_url + (f"&X-Bogus={x_bogus}" if x_bogus else "")

                status_code, body = _do_fetch(signed_url)
                logger.info(
                    "hidden_api %s -> HTTP %s, body length %d",
                    endpoint_key,
                    status_code or "?",
                    len(body or ""),
                )

                if not body or len(body) < 10:
                    if attempt < max_retries:
                        delay = random.uniform(*sleep_range)
                        logger.warning(
                            "hidden_api %s empty/small body (attempt %d/%d). Sleeping %.1fs…",
                            endpoint_key,
                            attempt,
                            max_retries,
                            delay,
                        )
                        try:
                            self.go(TIKTOK_BASE + "/foryou", wait=2.5)
                        except Exception:
                            pass
                        time.sleep(delay)
                        continue
                    return None

                data = json.loads(body)

                # Accept if statusCode == 0 OR payload contains expected keys
                if data.get("statusCode") == 0 or any(k in data for k in _expected_keys(endpoint_key)):
                    return data

                # Otherwise treat as transient API error and retry
                status_code = data.get("statusCode")

                if endpoint_key == "hashtag_videos" and status_code == 100002:
                    logger.warning(
                        "[%s] API blocked by TikTok (statusCode=100002).",
                        endpoint_key,
                    )
                    return None

                if attempt < max_retries:
                    delay = random.uniform(*sleep_range)
                    logger.warning(
                        "[%s] API error statusCode=%s (attempt %d/%d). Sleeping %.1fs…",
                        endpoint_key,
                        status_code,
                        attempt,
                        max_retries,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                return None

            except json.JSONDecodeError as exc:
                if attempt < max_retries:
                    delay = random.uniform(*sleep_range)
                    logger.warning(
                        "[%s] JSON decode error (attempt %d/%d): %s. Sleeping %.1fs…",
                        endpoint_key,
                        attempt,
                        max_retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                return None
            except Exception as exc:
                if attempt < max_retries:
                    delay = random.uniform(*sleep_range)
                    logger.warning(
                        "[%s] Request error (attempt %d/%d): %s. Sleeping %.1fs…",
                        endpoint_key,
                        attempt,
                        max_retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                logger.error("Unexpected error in fetch_api: %s", exc)
                return None

        return None


# ===========================================================================
# PyktokAPI class
# ===========================================================================
class PyktokAPI:
    """TikTok Hidden API scraper with CSV export support."""

    def __init__(
        self,
        *,
        browser_name: Optional[str] = None,
        headless: bool = True,
    ):
        self.browser_name = browser_name
        self.headless = headless

        # Cookie jar
        self._cookies = None
        if browser_name:
            self._cookies = getattr(browser_cookie3, browser_name)(domain_name=".tiktok.com")

        # Selenium driver
        self._driver: Optional[SeleniumDriverAPI] = None

        # Cache the last successful responses so CSV save helpers can still
        # write non-empty output when a refetch is transiently blocked.
        self._last_ok: Dict[str, Any] = {}

    def _cache_set(self, key: str, value: Any):
        if value is None:
            return
        if isinstance(value, list) and not value:
            return
        if isinstance(value, dict) and not value:
            return
        self._last_ok[key] = value

    def _cache_get(self, key: str, default: Any):
        return self._last_ok.get(key, default)

    def _ensure_driver(self) -> SeleniumDriverAPI:
        if self._driver is None or self._driver.driver is None:
            self._driver = SeleniumDriverAPI(headless=self.headless)
            self._driver.start(cookies_jar=self._cookies)
        return self._driver

    def close(self):
        if self._driver:
            self._driver.stop()
            self._driver = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # =====================================================================
    # Hidden API methods
    # =====================================================================

    def get_user_info(self, username: str) -> Optional[Dict]:
        """Return raw user-detail JSON."""
        drv = self._ensure_driver()
        username = (username or "").strip().lstrip("@").strip()

        # Visit user page first to warm up context
        try:
            logger.info(f"Visiting @{username} page to warm up context...")
            drv.go(f"{TIKTOK_BASE}/@{username}", wait=3.0)
            time.sleep(random.uniform(2.0, 3.0))
        except Exception as e:
            logger.warning(f"Page visit failed (continuing anyway): {e}")

        out = drv.fetch_api("user_detail", {"uniqueId": username, "secUid": ""})
        self._cache_set(f"user_detail:{username}", out)
        return out

    def get_user_videos(self, username: str, count: int = 30) -> List[Dict]:
        """Return up to *count* video dicts for a user."""
        drv = self._ensure_driver()
        username = (username or "").strip().lstrip("@").strip()

        # Get sec_uid
        user_resp = self.get_user_info(username)
        if not user_resp:
            logger.error("Could not fetch user info for %s", username)
            return []
        sec_uid = _safe(user_resp, "userInfo", "user", "secUid", default="")
        if not sec_uid:
            logger.error("No secUid in user response for %s", username)
            return []

        videos: List[Dict] = []
        cursor = 0
        consecutive_failures = 0
        max_consecutive_failures = 4

        while len(videos) < count:
            resp = drv.fetch_api("user_videos", {
                "secUid": sec_uid,
                "count": 35,
                "cursor": cursor,
            })
            if not resp:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.warning(
                        "Hidden API user_videos failed %d times; returning partial result (%d videos).",
                        consecutive_failures,
                        len(videos),
                    )
                    return videos[:count]

                logger.warning(
                    "Hidden API user_videos transient failure (%d/%d); refreshing session and retrying.",
                    consecutive_failures,
                    max_consecutive_failures,
                )
                time.sleep(random.uniform(3.0, 6.0))
                drv._hidden_api_session_params = None
                try:
                    drv.go(f"{TIKTOK_BASE}/@{username}", wait=2.5)
                    time.sleep(random.uniform(1.5, 3.0))
                except Exception:
                    pass
                continue

            consecutive_failures = 0
            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)

        out = videos[:count]
        self._cache_set(f"user_videos:{username}", out)
        return out

    def get_hashtag_videos(self, hashtag: str, count: int = 30) -> List[Dict]:
        """Return up to *count* video dicts for a hashtag."""
        drv = self._ensure_driver()
        hashtag = (hashtag or "").strip().lstrip("#").strip()

        # Get challenge ID
        detail = drv.fetch_api("hashtag_detail", {"challengeName": hashtag})
        if not detail:
            return []
        challenge_id = _safe(detail, "challengeInfo", "challenge", "id", default="")
        if not challenge_id:
            logger.error("No challengeID for hashtag %s", hashtag)
            return []

        videos: List[Dict] = []
        cursor = 0
        blocked_retries = 0
        max_blocked_retries = 3

        while len(videos) < count:
            resp = drv.fetch_api("hashtag_videos", {
                "challengeID": challenge_id,
                "challengeName": hashtag,
                "count": 35,
                "cursor": cursor,
            })
            if not resp:
                blocked_retries += 1
                if blocked_retries > max_blocked_retries:
                    logger.warning(
                        "Hidden API hashtag_videos failed after %d retries; trying search fallback.",
                        max_blocked_retries,
                    )
                    fallback_videos = self.search_videos(f"#{hashtag}", count)
                    if not fallback_videos:
                        fallback_videos = self.search_videos(hashtag, count)
                    return (videos + fallback_videos)[:count]

                logger.warning(
                    "Hidden API hashtag_videos blocked; refreshing API session and retrying (%d/%d).",
                    blocked_retries,
                    max_blocked_retries,
                )
                drv._hidden_api_session_params = None
                try:
                    drv.go(f"{TIKTOK_BASE}/tag/{hashtag}", wait=3.0)
                    time.sleep(random.uniform(1.0, 2.0))
                except Exception:
                    pass
                continue

            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)

        out = videos[:count]
        self._cache_set(f"hashtag_videos:{hashtag}", out)
        return out

    def get_video_comments(self, video_id: str, count: int = 30) -> List[Dict]:
        """Return up to *count* comment dicts for a video."""
        drv = self._ensure_driver()

        # Extract video ID
        m = re.search(VIDEO_ID_REGEX, str(video_id))
        vid = m.group(1) if m else str(video_id)

        comments: List[Dict] = []
        cursor = 0
        while len(comments) < count:
            resp = drv.fetch_api("video_comments", {
                "aweme_id": vid,
                "count": 20,
                "cursor": cursor,
            })
            if not resp:
                logger.warning("Hidden API video_comments failed.")
                return comments[:count]
            for c in resp.get("comments", []):
                comments.append(c)
                if len(comments) >= count:
                    break
            if not resp.get("has_more", False):
                break
            cursor = resp.get("cursor", 0)

        out = comments[:count]
        self._cache_set(f"video_comments:{vid}", out)
        return out

    def get_related_videos(self, video_id: str, count: int = 16) -> List[Dict]:
        """Return related-video dicts."""
        drv = self._ensure_driver()
        m = re.search(VIDEO_ID_REGEX, str(video_id))
        vid = m.group(1) if m else str(video_id)

        # Warm up the video page context when a full URL is provided.
        if isinstance(video_id, str) and "tiktok.com" in video_id:
            try:
                drv.go(video_id, wait=2.5)
                time.sleep(random.uniform(1.0, 2.0))
            except Exception:
                pass

        resp = drv.fetch_api("related_videos", {"itemID": vid, "count": count})
        if not resp:
            return []
        out = resp.get("itemList", [])[:count]
        self._cache_set(f"related_videos:{vid}", out)
        return out

    def get_trending_videos(self, count: int = 30) -> List[Dict]:
        """Return trending video dicts."""
        drv = self._ensure_driver()
        resp = drv.fetch_api("trending", {"from_page": "fyp", "count": count})
        if not resp:
            return []
        out = resp.get("itemList", [])[:count]
        self._cache_set("trending", out)
        return out

    def search_users(self, keyword: str, count: int = 10) -> List[Dict]:
        """Search TikTok for users matching *keyword*."""
        drv = self._ensure_driver()
        resp = drv.fetch_api("search_users", {"keyword": keyword, "cursor": 0, "from_page": "search"})
        if not resp:
            return []
        out = [u.get("user_info", {}) for u in resp.get("user_list", [])][:count]
        self._cache_set(f"search_users:{keyword}", out)
        return out

    def search_videos(self, keyword: str, count: int = 30) -> List[Dict]:
        """Search TikTok for videos matching *keyword*."""
        drv = self._ensure_driver()

        def _extract_videos(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []

            for item in resp.get("item_list", []) if isinstance(resp.get("item_list"), list) else []:
                if isinstance(item, dict):
                    out.append(item)

            for item in resp.get("itemList", []) if isinstance(resp.get("itemList"), list) else []:
                if isinstance(item, dict):
                    out.append(item)

            data_list = resp.get("data", [])
            if isinstance(data_list, list):
                for entry in data_list:
                    if not isinstance(entry, dict):
                        continue
                    candidate = entry.get("item")
                    if isinstance(candidate, dict):
                        out.append(candidate)
                    else:
                        candidate = entry.get("item_info") or entry.get("itemInfo")
                        if isinstance(candidate, dict):
                            out.append(candidate)

            return out

        videos: List[Dict] = []
        cursor = 0
        while len(videos) < count:
            resp = drv.fetch_api("search_general", {
                "keyword": keyword,
                "cursor": cursor,
                "from_page": "search",
                "web_search_code": '{"tiktok":{"client_params_x":{"search_engine":{"ies_mt_user_live_video_card_use_libra":1,"mt_search_general_user_live_card":1}},"search_server":{}}}',
            })
            if not resp:
                break

            items = _extract_videos(resp)
            if not items:
                break

            for item in items:
                videos.append(item)
                if len(videos) >= count:
                    break

            if not resp.get("has_more", False):
                break
            cursor = resp.get("cursor", 0)

        out = videos[:count]
        self._cache_set(f"search_videos:{keyword}", out)
        return out

    def get_sound_info(self, sound_id: str) -> Optional[Dict]:
        """Return sound-detail JSON."""
        drv = self._ensure_driver()
        out = drv.fetch_api("sound_detail", {"musicId": sound_id})
        self._cache_set(f"sound_detail:{sound_id}", out)
        return out

    def get_sound_videos(self, sound_id: str, count: int = 30) -> List[Dict]:
        """Return videos using a particular sound."""
        drv = self._ensure_driver()
        videos: List[Dict] = []
        cursor = 0
        while len(videos) < count:
            resp = drv.fetch_api("sound_videos", {"musicID": sound_id, "count": 30, "cursor": cursor})
            if not resp:
                break
            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)
        out = videos[:count]
        self._cache_set(f"sound_videos:{sound_id}", out)
        return out

    def get_playlist_info(self, playlist_id: str) -> Optional[Dict]:
        """Return playlist-detail JSON."""
        drv = self._ensure_driver()
        out = drv.fetch_api("playlist_detail", {"mixId": playlist_id})
        self._cache_set(f"playlist_detail:{playlist_id}", out)
        return out

    def get_playlist_videos(self, playlist_id: str, count: int = 30) -> List[Dict]:
        """Return videos in a playlist."""
        drv = self._ensure_driver()
        videos: List[Dict] = []
        cursor = 0
        while len(videos) < count:
            resp = drv.fetch_api("playlist_videos", {"mixId": playlist_id, "count": 30, "cursor": cursor})
            if not resp:
                break
            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)
        out = videos[:count]
        self._cache_set(f"playlist_videos:{playlist_id}", out)
        return out

    # =====================================================================
    # CSV Export Methods
    # =====================================================================

    def save_user_videos_csv(self, username: str, count: int = 30, filename: Optional[str] = None) -> str:
        """Fetch user videos and save to CSV."""
        if filename is None:
            filename = _default_csv_filename("user_videos", username)

        videos = self.get_user_videos(username, count)
        if not videos:
            cached = self._cache_get(f"user_videos:{(username or '').strip().lstrip('@').strip()}", [])
            if cached:
                logger.warning("User videos refetch failed; saving cached user videos (%d items).", len(cached))
                videos = cached[:count]
        if not videos:
            logger.warning("No videos found for user %s", username)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df_list = [generate_video_data_row(v) for v in videos]
        combined = pd.concat(df_list, ignore_index=True)
        combined = _deduplicate(filename, combined, key="video_id")
        combined.to_csv(filename, index=False)
        logger.info("Saved %d user videos to %s", len(combined), filename)
        return filename

    def save_hashtag_videos_csv(self, hashtag: str, count: int = 30, filename: Optional[str] = None) -> str:
        """Fetch hashtag videos and save to CSV."""
        if filename is None:
            filename = _default_csv_filename("hashtag_videos", hashtag)

        videos = self.get_hashtag_videos(hashtag, count)
        if not videos:
            cached = self._cache_get(f"hashtag_videos:{(hashtag or '').strip().lstrip('#').strip()}", [])
            if cached:
                logger.warning("Hashtag videos refetch failed; saving cached hashtag videos (%d items).", len(cached))
                videos = cached[:count]
        if not videos:
            logger.warning("No videos found for hashtag %s", hashtag)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df_list = [generate_video_data_row(v) for v in videos]
        combined = pd.concat(df_list, ignore_index=True)
        combined = _deduplicate(filename, combined, key="video_id")
        combined.to_csv(filename, index=False)
        logger.info("Saved %d hashtag videos to %s", len(combined), filename)
        return filename

    def save_video_comments_csv(self, video_id: str, count: int = 30, filename: Optional[str] = None) -> str:
        """Fetch video comments and save to CSV."""
        m = re.search(VIDEO_ID_REGEX, str(video_id))
        vid = m.group(1) if m else str(video_id)

        if filename is None:
            filename = _default_csv_filename("video_comments", str(video_id))

        comments = self.get_video_comments(video_id, count)
        if not comments:
            cached = self._cache_get(f"video_comments:{vid}", [])
            if cached:
                logger.warning("Comments refetch failed; saving cached comments (%d items).", len(cached))
                comments = cached[:count]
        if not comments:
            logger.warning("No comments found for video %s", video_id)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        rows = [_comment_export_row(c, aweme_id=vid) for c in comments]
        df = pd.DataFrame(rows)
        df = _deduplicate(filename, df, key="cid")
        df.to_csv(filename, index=False)
        logger.info("Saved %d comments to %s", len(df), filename)
        return filename

    def save_trending_videos_csv(self, count: int = 30, filename: Optional[str] = None) -> str:
        """Fetch trending videos and save to CSV."""
        if filename is None:
            filename = _default_csv_filename("trending")

        videos = self.get_trending_videos(count)
        if not videos:
            cached = self._cache_get("trending", [])
            if cached:
                logger.warning("Trending refetch failed; saving cached trending videos (%d items).", len(cached))
                videos = cached[:count]
        if not videos:
            logger.warning("No trending videos found")
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df_list = [generate_video_data_row(v) for v in videos]
        combined = pd.concat(df_list, ignore_index=True)
        combined = _deduplicate(filename, combined, key="video_id")
        combined.to_csv(filename, index=False)
        logger.info("Saved %d trending videos to %s", len(combined), filename)
        return filename

    def save_related_videos_csv(self, video_id: str, count: int = 16, filename: Optional[str] = None) -> str:
        """Fetch related videos and save to CSV."""
        m = re.search(VIDEO_ID_REGEX, str(video_id))
        vid = m.group(1) if m else str(video_id)

        if filename is None:
            filename = _default_csv_filename("related_videos", str(video_id))

        videos = self.get_related_videos(video_id, count)
        if not videos:
            cached = self._cache_get(f"related_videos:{vid}", [])
            if cached:
                logger.warning("Related refetch failed; saving cached related videos (%d items).", len(cached))
                videos = cached[:count]
        if not videos:
            logger.warning("No related videos found")
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df_list = [generate_video_data_row(v) for v in videos]
        combined = pd.concat(df_list, ignore_index=True)
        combined = _deduplicate(filename, combined, key="video_id")
        combined.to_csv(filename, index=False)
        logger.info("Saved %d related videos to %s", len(combined), filename)
        return filename

    def save_sound_videos_csv(self, sound_id: str, count: int = 30, filename: Optional[str] = None) -> str:
        """Fetch sound videos and save to CSV."""
        if filename is None:
            filename = _default_csv_filename("sound_videos", sound_id)

        videos = self.get_sound_videos(sound_id, count)
        if not videos:
            cached = self._cache_get(f"sound_videos:{sound_id}", [])
            if cached:
                logger.warning("Sound videos refetch failed; saving cached sound videos (%d items).", len(cached))
                videos = cached[:count]
        if not videos:
            logger.warning("No videos found for sound %s", sound_id)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df_list = [generate_video_data_row(v) for v in videos]
        combined = pd.concat(df_list, ignore_index=True)
        combined = _deduplicate(filename, combined, key="video_id")
        combined.to_csv(filename, index=False)
        logger.info("Saved %d sound videos to %s", len(combined), filename)
        return filename

    def save_playlist_videos_csv(self, playlist_id: str, count: int = 30, filename: Optional[str] = None) -> str:
        """Fetch playlist videos and save to CSV."""
        if filename is None:
            filename = _default_csv_filename("playlist_videos", playlist_id)

        videos = self.get_playlist_videos(playlist_id, count)
        if not videos:
            cached = self._cache_get(f"playlist_videos:{playlist_id}", [])
            if cached:
                logger.warning("Playlist videos refetch failed; saving cached playlist videos (%d items).", len(cached))
                videos = cached[:count]
        if not videos:
            logger.warning("No videos found for playlist %s", playlist_id)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df_list = [generate_video_data_row(v) for v in videos]
        combined = pd.concat(df_list, ignore_index=True)
        combined = _deduplicate(filename, combined, key="video_id")
        combined.to_csv(filename, index=False)
        logger.info("Saved %d playlist videos to %s", len(combined), filename)
        return filename

    def save_search_videos_csv(self, keyword: str, count: int = 30, filename: Optional[str] = None) -> str:
        """Search videos and save to CSV."""
        if filename is None:
            filename = _default_csv_filename("search_videos", keyword)

        videos = self.search_videos(keyword, count)
        if not videos:
            cached = self._cache_get(f"search_videos:{keyword}", [])
            if cached:
                logger.warning("Search videos refetch failed; saving cached search videos (%d items).", len(cached))
                videos = cached[:count]
        if not videos:
            logger.warning("No videos found for search: %s", keyword)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df_list = [generate_video_data_row(v) for v in videos]
        combined = pd.concat(df_list, ignore_index=True)
        combined = _deduplicate(filename, combined, key="video_id")
        combined.to_csv(filename, index=False)
        logger.info("Saved %d search videos to %s", len(combined), filename)
        return filename

    def save_user_info_csv(self, username: str, filename: Optional[str] = None) -> str:
        """Fetch user_detail and save (flattened) to CSV."""
        if filename is None:
            filename = _default_csv_filename("user_info", username)

        data = self.get_user_info(username)
        if not data:
            logger.warning("No user info found for %s", username)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df = pd.json_normalize(data, sep=".")
        # append + dedup where possible
        if os.path.exists(filename):
            old = pd.read_csv(filename, keep_default_na=False)
            df = pd.concat([old, df], ignore_index=True)
        for key in ("userInfo.user.id", "userInfo.user.uniqueId", "userInfo.user.secUid"):
            if key in df.columns:
                df[key] = df[key].astype(str)
                df = df.drop_duplicates(subset=[key])
                break
        df.to_csv(filename, index=False)
        logger.info("Saved user info to %s", filename)
        return filename

    def save_search_users_csv(self, keyword: str, count: int = 10, filename: Optional[str] = None) -> str:
        """Search users and save results to CSV."""
        if filename is None:
            filename = _default_csv_filename("search_users", keyword)

        users = self.search_users(keyword, count=count)
        if not users:
            cached = self._cache_get(f"search_users:{keyword}", [])
            if cached:
                logger.warning("Search users refetch failed; saving cached search users (%d items).", len(cached))
                users = cached[:count]
        if not users:
            logger.warning("No users found for search: %s", keyword)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df = pd.json_normalize(users, sep=".")
        if os.path.exists(filename):
            old = pd.read_csv(filename, keep_default_na=False)
            df = pd.concat([old, df], ignore_index=True)
        for key in ("uniqueId", "unique_id", "uid", "user.uniqueId", "user.unique_id"):
            if key in df.columns:
                df[key] = df[key].astype(str)
                df = df.drop_duplicates(subset=[key])
                break
        df.to_csv(filename, index=False)
        logger.info("Saved %d search users to %s", len(df), filename)
        return filename

    def save_sound_info_csv(self, sound_id: str, filename: Optional[str] = None) -> str:
        """Fetch sound_detail and save (flattened) to CSV."""
        if filename is None:
            filename = _default_csv_filename("sound_info", sound_id)

        data = self.get_sound_info(sound_id)
        if not data:
            logger.warning("No sound info found for %s", sound_id)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df = pd.json_normalize(data, sep=".")
        if os.path.exists(filename):
            old = pd.read_csv(filename, keep_default_na=False)
            df = pd.concat([old, df], ignore_index=True)
        for key in ("musicInfo.music.id", "musicInfo.music.title"):
            if key in df.columns:
                df[key] = df[key].astype(str)
                df = df.drop_duplicates(subset=[key])
                break
        df.to_csv(filename, index=False)
        logger.info("Saved sound info to %s", filename)
        return filename

    def save_playlist_info_csv(self, playlist_id: str, filename: Optional[str] = None) -> str:
        """Fetch playlist_detail and save (flattened) to CSV."""
        if filename is None:
            filename = _default_csv_filename("playlist_info", playlist_id)

        data = self.get_playlist_info(playlist_id)
        if not data:
            logger.warning("No playlist info found for %s", playlist_id)
            pd.DataFrame().to_csv(filename, index=False)
            return filename

        df = pd.json_normalize(data, sep=".")
        if os.path.exists(filename):
            old = pd.read_csv(filename, keep_default_na=False)
            df = pd.concat([old, df], ignore_index=True)
        for key in ("mixInfo.mixId", "mixInfo.mix.id", "mixInfo.title"):
            if key in df.columns:
                df[key] = df[key].astype(str)
                df = df.drop_duplicates(subset=[key])
                break
        df.to_csv(filename, index=False)
        logger.info("Saved playlist info to %s", filename)
        return filename


# ===========================================================================
# Module-level singleton API
# ===========================================================================
_api: Optional[PyktokAPI] = None


def _get_api() -> PyktokAPI:
    """Return the active singleton."""
    global _api
    if _api is None:
        _api = PyktokAPI()
        logger.warning("No browser specified yet. Call specify_browser('chrome') first.")
    return _api


def specify_browser(browser: str = "chrome", *, headless: bool = True):
    """Initialize PyktokAPI with browser cookies."""
    global _api
    if _api is not None:
        _api.close()
    _api = PyktokAPI(browser_name=browser, headless=headless)


def get_user_info(username: str) -> Optional[Dict]:
    """Return raw user-detail JSON."""
    return _get_api().get_user_info(username)


def get_user_videos(username: str, count: int = 30) -> List[Dict]:
    """Return up to *count* video dicts for a user."""
    return _get_api().get_user_videos(username, count)


def get_hashtag_videos(hashtag: str, count: int = 30) -> List[Dict]:
    """Return up to *count* video dicts for a hashtag."""
    return _get_api().get_hashtag_videos(hashtag, count)


def get_video_comments(video_id: str, count: int = 30) -> List[Dict]:
    """Return up to *count* comment dicts for a video."""
    return _get_api().get_video_comments(video_id, count)


def get_related_videos(video_id: str, count: int = 16) -> List[Dict]:
    """Return related-video dicts."""
    return _get_api().get_related_videos(video_id, count)


def get_trending_videos(count: int = 30) -> List[Dict]:
    """Return trending video dicts."""
    return _get_api().get_trending_videos(count)


def search_users(keyword: str, count: int = 10) -> List[Dict]:
    """Search TikTok for users matching *keyword*."""
    return _get_api().search_users(keyword, count)


def search_videos(keyword: str, count: int = 30) -> List[Dict]:
    """Search TikTok for videos matching *keyword*."""
    return _get_api().search_videos(keyword, count)


def get_sound_info(sound_id: str) -> Optional[Dict]:
    """Return sound-detail JSON."""
    return _get_api().get_sound_info(sound_id)


def get_sound_videos(sound_id: str, count: int = 30) -> List[Dict]:
    """Return videos using a particular sound."""
    return _get_api().get_sound_videos(sound_id, count)


def get_playlist_info(playlist_id: str) -> Optional[Dict]:
    """Return playlist-detail JSON."""
    return _get_api().get_playlist_info(playlist_id)


def get_playlist_videos(playlist_id: str, count: int = 30) -> List[Dict]:
    """Return videos in a playlist."""
    return _get_api().get_playlist_videos(playlist_id, count)


# CSV Export Functions
def save_user_videos_csv(username: str, count: int = 30, filename: Optional[str] = None) -> str:
    """Fetch user videos and save to CSV."""
    return _get_api().save_user_videos_csv(username, count, filename)


def save_hashtag_videos_csv(hashtag: str, count: int = 30, filename: Optional[str] = None) -> str:
    """Fetch hashtag videos and save to CSV."""
    return _get_api().save_hashtag_videos_csv(hashtag, count, filename)


def save_video_comments_csv(video_id: str, count: int = 30, filename: Optional[str] = None) -> str:
    """Fetch video comments and save to CSV."""
    return _get_api().save_video_comments_csv(video_id, count, filename)


def save_trending_videos_csv(count: int = 30, filename: Optional[str] = None) -> str:
    """Fetch trending videos and save to CSV."""
    return _get_api().save_trending_videos_csv(count, filename)


def save_related_videos_csv(video_id: str, count: int = 16, filename: Optional[str] = None) -> str:
    """Fetch related videos and save to CSV."""
    return _get_api().save_related_videos_csv(video_id, count, filename)


def save_sound_videos_csv(sound_id: str, count: int = 30, filename: Optional[str] = None) -> str:
    """Fetch sound videos and save to CSV."""
    return _get_api().save_sound_videos_csv(sound_id, count, filename)


def save_playlist_videos_csv(playlist_id: str, count: int = 30, filename: Optional[str] = None) -> str:
    """Fetch playlist videos and save to CSV."""
    return _get_api().save_playlist_videos_csv(playlist_id, count, filename)


def save_search_videos_csv(keyword: str, count: int = 30, filename: Optional[str] = None) -> str:
    """Search videos and save to CSV."""
    return _get_api().save_search_videos_csv(keyword, count, filename)


def save_user_info_csv(username: str, filename: Optional[str] = None) -> str:
    """Fetch user info and save (flattened) to CSV."""
    return _get_api().save_user_info_csv(username, filename)


def save_search_users_csv(keyword: str, count: int = 10, filename: Optional[str] = None) -> str:
    """Search users and save results to CSV."""
    return _get_api().save_search_users_csv(keyword, count, filename)


def save_sound_info_csv(sound_id: str, filename: Optional[str] = None) -> str:
    """Fetch sound info and save (flattened) to CSV."""
    return _get_api().save_sound_info_csv(sound_id, filename)


def save_playlist_info_csv(playlist_id: str, filename: Optional[str] = None) -> str:
    """Fetch playlist info and save (flattened) to CSV."""
    return _get_api().save_playlist_info_csv(playlist_id, filename)


def close():
    """Shut down the browser session."""
    global _api
    if _api:
        _api.close()
        _api = None
