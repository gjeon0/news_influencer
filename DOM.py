# -*- coding: utf-8 -*-
"""
updated_pyktok_selenium_020426.py  –  Pyktok
============================================
A simple module to collect video, text, and metadata from TikTok.

Pyktok pulls data from the JSON objects embedded in TikTok pages (scraping,
the default) and optionally from hidden APIs with no public documentation
(set use_hidden_api=True).

---------------------------------------------------------------------------
USAGE  –  every operation is a single function call at module level
---------------------------------------------------------------------------
import updated_pyktok_selenium_020426 as pyk

# 0. Initialise once – tell Pyktok which browser's cookies to borrow
pyk.specify_browser('chrome')            # or 'firefox', 'edge', …

# --- single video --------------------------------------------------------
pyk.save_tiktok(URL, save_video=True, metadata_fn='data.csv')
tt_json = pyk.get_tiktok_json(URL)       # full JSON object

# --- bulk from a page -----------------------------------------------------
pyk.save_tiktok_multi_page('tiktok',  ent_type='user',         metadata_fn='user.csv')
pyk.save_tiktok_multi_page('science', ent_type='hashtag',      metadata_fn='ht.csv')
pyk.save_tiktok_multi_page(URL,       ent_type='video_related',metadata_fn='rel.csv')

# --- list of URLs ---------------------------------------------------------
pyk.save_tiktok_multi_urls([URL1, URL2], save_video=False, metadata_fn='multi.csv')

# --- comments -------------------------------------------------------------
pyk.save_tiktok_comments(URL, comment_count=30, save_comments=True)
comments_df = pyk.get_tiktok_comments(URL, comment_count=30)

# --- hidden-API endpoints (set use_hidden_api=True first) -----------------
pyk.specify_browser('chrome', use_hidden_api=True)
videos   = pyk.get_user_videos('therock', count=50)
ht_vids  = pyk.get_hashtag_videos('tylenol', count=20)
comments = pyk.get_video_comments(VIDEO_URL, count=40)
trending = pyk.get_trending_videos(count=30)
related  = pyk.get_related_videos(VIDEO_URL, count=16)
users    = pyk.search_users('keyword', count=10)
sound    = pyk.get_sound_info('7016547803243022337')
s_vids   = pyk.get_sound_videos('7016547803243022337', count=20)
pl_info  = pyk.get_playlist_info('7426714779919797038')
pl_vids  = pyk.get_playlist_videos('7426714779919797038', count=30)

---------------------------------------------------------------------------
CLI
---------------------------------------------------------------------------
python updated_pyktok_selenium_020426.py user therock 30
python updated_pyktok_selenium_020426.py --hidden-api hashtag tylenol 20
python updated_pyktok_selenium_020426.py --selenium --download-videos video URL
python updated_pyktok_selenium_020426.py --hidden-api comments URL 40
"""

from __future__ import annotations

import os
import sys
import time
import random
import json
import re
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from urllib.parse import urlencode, quote

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from bs4 import BeautifulSoup
import browser_cookie3
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pyktok")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TIKTOK_BASE = "https://www.tiktok.com"

HEADERS = {
    "Accept-Encoding": "gzip, deflate, sdch",
    "Accept-Language": "en-US,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
}

URL_REGEX = r"(?<=\.com/)(.+?)(?=\?|$)"
VIDEO_ID_REGEX = r"(?<=/video/)([0-9]+)"

# ---------------------------------------------------------------------------
# Stealth helpers (injected into Selenium page)
# ---------------------------------------------------------------------------
_STEALTH_SCRIPT = """
(function(){
    // Hide webdriver flag
    delete Object.getPrototypeOf(navigator).webdriver;

    // Fake chrome runtime
    if(!window.chrome){window.chrome={};}
    if(!window.chrome.runtime){
        window.chrome.runtime={id:undefined,connect:null,sendMessage:null};
    }

    // Navigator overrides
    var nd = Object.getPrototypeOf(navigator);
    Object.defineProperty(nd,'languages',{get:()=>['en-US','en']});
    Object.defineProperty(nd,'platform',{get:()=>'MacIntel'});
    Object.defineProperty(nd,'vendor',{get:()=>'Google Inc.'});

    // WebGL vendor spoof
    var gp = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(p){
        if(p===37445) return 'Intel Inc.';
        if(p===37446) return 'Intel Iris OpenGL Engine';
        return gp.apply(this,arguments);
    };
})();
"""


# ===========================================================================
# SeleniumDriver  –  thin wrapper around undetected_chromedriver
# ===========================================================================
class SeleniumDriver:
    """Manages a single undetected Chrome instance with stealth & cookie support."""

    def __init__(self, headless: bool = True, fast_mode: bool = True):
        self.headless = headless
        self.fast_mode = fast_mode
        self.driver: Optional[uc.Chrome] = None

    # --- lifecycle --------------------------------------------------------
    def start(self, cookies_jar=None):
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
        if self.fast_mode:
            prefs["profile.default_content_setting_values.images"] = 2
        opts.add_experimental_option("prefs", prefs)

        self.driver = uc.Chrome(options=opts, use_subprocess=False, version_main=144)
        time.sleep(0.5)

        # Inject stealth JS before first navigation
        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument", {"source": _STEALTH_SCRIPT}
            )
        except Exception:
            pass

        # Block heavy resources when fast_mode
        if self.fast_mode:
            try:
                self.driver.execute_cdp_cmd("Network.enable", {})
                self.driver.execute_cdp_cmd("Network.setBlockedURLs", {
                    "urls": ["*.jpg","*.jpeg","*.png","*.gif","*.webp","*.svg",
                             "*.mp4","*.webm","*.woff","*.woff2","*.ttf"]
                })
            except Exception:
                pass

        # Land on TikTok and load cookies so subsequent requests look authentic
        self.driver.get(TIKTOK_BASE)
        time.sleep(2)
        if cookies_jar:
            self._inject_cookies(cookies_jar)

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

    # --- navigation -------------------------------------------------------
    def go(self, url: str, wait: float = 2.0):
        """Navigate to *url* with a small random jitter."""
        self.driver.get(url)
        time.sleep(wait + random.uniform(0.0, 0.8))
        # Simulate a tiny mouse move so TikTok doesn't flag as bot
        self.driver.execute_script(
            "document.dispatchEvent(new MouseEvent('mousemove',"
            "{clientX:%d,clientY:%d}));" % (random.randint(10,200), random.randint(10,200))
        )

    # --- cookie helpers ---------------------------------------------------
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

    # --- page content -----------------------------------------------------
    def page_source(self) -> str:
        return self.driver.page_source

    def scroll_to_bottom(self):
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")


# ===========================================================================
# JSON extraction helpers  (page-level, works for requests & selenium modes)
# ===========================================================================
def _extract_json_from_html(html: str) -> Optional[Dict[str, Any]]:
    """Pull __UNIVERSAL_DATA_FOR_REHYDRATION__ (preferred) or SIGI_STATE JSON."""
    soup = BeautifulSoup(html, "html.parser")
    for script_id in ("__UNIVERSAL_DATA_FOR_REHYDRATION__", "SIGI_STATE"):
        tag = soup.find("script", {"id": script_id})
        if tag and tag.string:
            try:
                return json.loads(tag.string)
            except json.JSONDecodeError:
                continue
    return None


def _extract_video_struct(tt_json: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict]]:
    """
    Given the top-level JSON blob, return (video_id, item_struct_dict).
    Handles both legacy ItemModule layout and current __DEFAULT_SCOPE__ layout.
    """
    # --- current format ---------------------------------------------------
    if "__DEFAULT_SCOPE__" in tt_json:
        vd = tt_json["__DEFAULT_SCOPE__"].get("webapp.video-detail", {})
        item_info = vd.get("itemInfo")
        if item_info is None:
            code = vd.get("statusCode")
            if code:
                logger.warning("TikTok status %s: %s", code, vd.get("statusMsg"))
            return None, None
        struct = item_info.get("itemStruct", {})
        return struct.get("id"), struct

    # --- legacy format ----------------------------------------------------
    if "ItemModule" in tt_json and tt_json["ItemModule"]:
        vid_id = next(iter(tt_json["ItemModule"]))
        return vid_id, tt_json["ItemModule"][vid_id]

    return None, None


# ===========================================================================
# Data-row builder  –  produces a single-row DataFrame matching pyktok CSV format
# ===========================================================================
DATA_COLUMNS = [
    "video_id", "video_timestamp", "video_duration", "video_locationcreated",
    "video_diggcount", "video_sharecount", "video_commentcount", "video_playcount",
    "video_description", "video_is_ad", "video_stickers",
    "author_username", "author_name",
    "author_followercount", "author_followingcount", "author_heartcount",
    "author_videocount", "author_diggcount", "author_verified",
    "poi_name", "poi_address", "poi_city",
]


def _safe(obj, *keys, default=""):
    """Drill into nested dicts safely."""
    for k in keys:
        if isinstance(obj, dict):
            obj = obj.get(k)
        else:
            return default
    return obj if obj is not None else default


def generate_data_row(video_obj: Dict[str, Any]) -> pd.DataFrame:
    """Convert a TikTok video-object dict into a single-row DataFrame."""
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


def _deduplicate(existing_path: str, new_df: pd.DataFrame, key: str = "video_id") -> pd.DataFrame:
    """Merge *new_df* with an existing CSV (if present) and drop duplicates on *key*."""
    if os.path.exists(existing_path):
        old = pd.read_csv(existing_path, keep_default_na=False)
        new_df = pd.concat([old, new_df], ignore_index=True)
    new_df[key] = new_df[key].astype(str)
    return new_df.drop_duplicates(subset=[key])


# ===========================================================================
# Pyktok  –  main façade
# ===========================================================================
class Pyktok:
    """
    All-in-one TikTok scraper controlled entirely by boolean hyperparameters.

    Parameters
    ----------
    use_selenium : bool (default False)
        False > plain HTTP + BeautifulSoup (fastest).
        True  > undetected Chrome; needed when HTTP is blocked.
    use_hidden_api : bool (default False)
        True  > call TikTok's internal JSON endpoints (requires a browser session
                for X-Bogus signing; implicitly enables Selenium).
    download_videos : bool (default False)
        True  > download .mp4 / image files in addition to metadata.
    browser_name : str | None
        Browser to borrow cookies from (``"chrome"``, ``"firefox"`` …).
    headless : bool (default True)
        Run the browser headless.
    """

    def __init__(
        self,
        *,
        use_selenium: bool = False,
        use_hidden_api: bool = False,
        download_videos: bool = False,
        browser_name: Optional[str] = None,
        headless: bool = True,
    ):
        self.use_hidden_api  = use_hidden_api
        self.use_selenium    = use_selenium or use_hidden_api  # hidden_api needs a browser
        self.download_videos = download_videos
        self.browser_name    = browser_name
        self.headless        = headless

        # cookie jar (shared across requests & selenium modes)
        self._cookies = None
        if browser_name:
            self._cookies = getattr(browser_cookie3, browser_name)(domain_name=".tiktok.com")

        # Selenium driver – created lazily only when needed
        self._driver: Optional[SeleniumDriver] = None

    # --- driver lifecycle -------------------------------------------------
    def _ensure_driver(self) -> SeleniumDriver:
        if self._driver is None or self._driver.driver is None:
            # hidden_api needs full page load (no resource blocking) for signing
            self._driver = SeleniumDriver(headless=self.headless, fast_mode=not self.use_hidden_api)
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
    # LOW-LEVEL HELPERS
    # =====================================================================

    def _requests_get_html(self, url: str) -> str:
        """Fetch a page with plain requests + cookies."""
        resp = requests.get(url, headers=HEADERS, cookies=self._cookies, timeout=25)
        # Keep cookie jar warm
        if self._cookies is not None:
            self._cookies = resp.cookies
        return resp.text

    def _selenium_get_html(self, url: str) -> str:
        drv = self._ensure_driver()
        drv.go(url)
        return drv.page_source()

    def _get_html(self, url: str) -> str:
        """Route to the correct backend based on boolean flags."""
        if not self.use_selenium:
            return self._requests_get_html(url)
        return self._selenium_get_html(url)

    # ----- hidden_api: sign & fetch ---------------------------------------
    def _hidden_api_fetch(self, endpoint_key: str, params: Dict[str, Any]) -> Optional[Dict]:
        """
        Sign *params* against a TikTok endpoint and execute the fetch inside the
        Selenium page context (so X-Bogus is generated by TikTok's own JS).

        Returns the parsed JSON response or None on failure.
        """
        drv = self._ensure_driver()

        path = HIDDEN_API_ENDPOINTS[endpoint_key]
        base_url = TIKTOK_BASE + path

        # Grab msToken from the live session
        ms_token = drv.get_ms_token()
        if ms_token:
            params["msToken"] = ms_token

        # Build query string
        query = urlencode(params, safe="=", quote_via=quote)
        full_url = f"{base_url}?{query}"

        # We need byted_acrawler loaded – ensure we are on a TikTok page
        try:
            drv.driver.execute_script("return window.byted_acrawler;")
        except Exception:
            drv.go(TIKTOK_BASE)
            time.sleep(2)

        # Generate X-Bogus via TikTok's own signing function
        try:
            sign_result = drv.driver.execute_script(
                "return window.byted_acrawler.frontierSign(arguments[0]);",
                full_url,
            )
            x_bogus = sign_result.get("X-Bogus", "") if isinstance(sign_result, dict) else ""
        except Exception as exc:
            logger.warning("X-Bogus generation failed: %s", exc)
            x_bogus = ""

        if x_bogus:
            full_url += f"&X-Bogus={x_bogus}"

        # Execute fetch() inside the page so cookies / origin are correct
        js = f"""
        return await (async () => {{
            const r = await fetch('{full_url}', {{
                method: 'GET',
                headers: {{
                    'Referer': '{TIKTOK_BASE}/',
                    'Origin':  '{TIKTOK_BASE}'
                }}
            }});
            return await r.text();
        }})();
        """
        try:
            raw = drv.driver.execute_async_script(js)
            return json.loads(raw)
        except Exception as exc:
            logger.error("hidden_api fetch failed for %s: %s", endpoint_key, exc)
            return None

    # =====================================================================
    # PUBLIC API  –  single video
    # =====================================================================
    def get_video_json(self, video_url: str) -> Tuple[Optional[str], Optional[Dict]]:
        """
        Fetch JSON data for a single video.
        Returns (video_id, item_struct).  Works in all three modes.
        """
        html = self._get_html(video_url)
        tt_json = _extract_json_from_html(html)
        if tt_json is None:
            logger.warning("No JSON found on page for %s", video_url)
            return None, None
        return _extract_video_struct(tt_json)

    def save_tiktok(
        self,
        video_url: str,
        *,
        save_video: Optional[bool] = None,   # None > falls back to self.download_videos
        metadata_fn: str = "",
        dir_path: Optional[str] = None,
    ):
        """
        Save a single TikTok video file and/or its metadata row.

        Parameters
        ----------
        video_url    : full TikTok video URL
        save_video   : download the .mp4 (or image slides).
                       Defaults to the instance-level ``download_videos`` flag.
        metadata_fn  : CSV path; if non-empty the metadata row is appended
        dir_path     : directory for the downloaded video file
        """
        if save_video is None:
            save_video = self.download_videos

        if not save_video and not metadata_fn:
            logger.info("Nothing to do – both save_video and metadata_fn are empty.")
            return None

        video_id, data_slot = self.get_video_json(video_url)
        if video_id is None or data_slot is None:
            logger.error("Could not extract video data from %s", video_url)
            return None

        content_paths: List[str] = []

        # --- download video / images --------------------------------------
        if save_video:
            regex_url = re.findall(URL_REGEX, video_url)
            base_name = regex_url[0].replace("/", "_") if regex_url else video_id

            if "imagePost" in data_slot:
                # Slideshow
                for i, slide in enumerate(data_slot["imagePost"].get("images", []), 1):
                    img_url = slide.get("imageURL", {}).get("urlList", [""])[0]
                    if img_url:
                        img_path = self._download_file(img_url, f"{base_name}_slide_{i}.jpeg", dir_path)
                        if img_path:
                            content_paths.append(img_path)
            else:
                # Single video
                video_obj = data_slot.get("video", {}) if isinstance(data_slot, dict) else {}
                candidate_urls: List[str] = []

                for key in ("downloadAddr", "playAddr"):
                    val = video_obj.get(key, "")
                    if isinstance(val, str) and val:
                        candidate_urls.append(val)
                    elif isinstance(val, dict):
                        for u in val.get("urlList", []) or val.get("UrlList", []):
                            if isinstance(u, str) and u:
                                candidate_urls.append(u)

                for bi in video_obj.get("bitrateInfo", []) or []:
                    if not isinstance(bi, dict):
                        continue
                    play_addr = bi.get("PlayAddr") or bi.get("playAddr") or {}
                    if isinstance(play_addr, dict):
                        for u in (play_addr.get("UrlList", []) or play_addr.get("urlList", [])):
                            if isinstance(u, str) and u:
                                candidate_urls.append(u)

                # De-duplicate while preserving order
                deduped_urls: List[str] = []
                seen_urls: set[str] = set()
                for u in candidate_urls:
                    if u not in seen_urls:
                        seen_urls.add(u)
                        deduped_urls.append(u)

                saved_ok = False
                for idx, media_url in enumerate(deduped_urls, start=1):
                    out_path = self._download_file(media_url, f"{base_name}.mp4", dir_path)
                    if out_path and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                        content_paths.append(out_path)
                        saved_ok = True
                        break
                    logger.warning("Video download candidate %d/%d failed for %s", idx, len(deduped_urls), video_url)

                if not deduped_urls:
                    logger.warning("No download URL found for %s", video_url)
                elif not saved_ok:
                    logger.warning("All download candidates failed for %s", video_url)

        # --- save metadata ------------------------------------------------
        if metadata_fn:
            row_df = generate_data_row(data_slot)
            combined = _deduplicate(metadata_fn, row_df)
            combined.to_csv(metadata_fn, index=False)
            logger.info("Metadata saved > %s", metadata_fn)

        return content_paths, metadata_fn

    # =====================================================================
    # PUBLIC API  –  bulk / listing
    # =====================================================================
    def get_video_urls(
        self,
        entity: str,
        entity_type: str = "user",
        count: int = 30,
    ) -> List[str]:
        """
        Collect *count* video URLs from a user profile, hashtag, or related-videos page.

        entity_type : "user" | "hashtag" | "video_related"
            For "video_related", *entity* must be a full video URL.
        """
        if entity_type == "user":
            url = f"{TIKTOK_BASE}/@{entity}"
        elif entity_type == "hashtag":
            url = f"{TIKTOK_BASE}/tag/{entity}"
        elif entity_type == "video_related":
            url = entity  # entity IS the video URL
        else:
            raise ValueError('entity_type must be "user", "hashtag", or "video_related"')

        drv = self._ensure_driver()
        drv.go(url, wait=3.0)

        video_urls: set = set()

        # --- initial JSON extraction (fast path for user pages) -----------
        html = drv.page_source()
        tt_json = _extract_json_from_html(html)
        if tt_json and "__DEFAULT_SCOPE__" in tt_json:
            scope = tt_json["__DEFAULT_SCOPE__"]
            if entity_type == "user":
                item_list = scope.get("webapp.user-detail", {}).get("itemList", [])
                video_urls.update(
                    f"{TIKTOK_BASE}/@{entity}/video/{vid}" for vid in item_list
                )
            elif entity_type == "hashtag":
                # hashtag pages sometimes embed initial items in ItemModule
                for vid_id in tt_json.get("ItemModule", {}):
                    author = _safe(tt_json["ItemModule"][vid_id], "author", "uniqueId", default="")
                    if author:
                        video_urls.add(f"{TIKTOK_BASE}/@{author}/video/{vid_id}")

        # --- scroll-and-scrape (DOM fallback) -----------------------------
        no_new = 0
        max_no_new = 30
        max_scrolls = max(300, count * 3)

        target_user = entity.strip().lstrip("@").lower() if entity_type == "user" else ""

        def _is_allowed_href(href: str) -> bool:
            if "/video/" not in href:
                return False
            if entity_type != "user":
                return True
            match = re.search(r"/@([^/]+)/video/", href)
            if not match:
                return False
            return match.group(1).strip().lower() == target_user

        for _ in range(max_scrolls):
            if len(video_urls) >= count:
                break

            prev = len(video_urls)
            hrefs = drv.driver.execute_script(
                "return Array.from(document.querySelectorAll('a[href*=\"/video/\"]')).map(a => a.href || '');"
            ) or []
            for href in hrefs:
                if _is_allowed_href(href):
                    video_urls.add(href)

            if len(video_urls) == prev:
                no_new += 1
                if no_new >= max_no_new:
                    logger.info("No new videos after %d scrolls – stopping.", max_no_new)
                    break
            else:
                no_new = 0

            drv.scroll_to_bottom()
            time.sleep(random.uniform(1.2, 2.0))

        return list(video_urls)[:count]

    def save_tiktok_multi_urls(
        self,
        video_urls: List[str],
        *,
        save_video: bool = False,
        metadata_fn: str = "",
        sleep: float = 4.0,
        dir_path: Optional[str] = None,
    ):
        """Download metadata (and optionally video files) for a list of URLs."""
        for url in video_urls:
            self.save_tiktok(url, save_video=save_video, metadata_fn=metadata_fn, dir_path=dir_path)
            time.sleep(random.uniform(0.5, sleep))
        logger.info("Processed %d URLs.", len(video_urls))

    def save_tiktok_multi_page(
        self,
        entity: str,
        entity_type: str = "user",
        count: int = 30,
        *,
        save_video: bool = False,
        metadata_fn: str = "",
        sleep: float = 4.0,
        dir_path: Optional[str] = None,
    ):
        """
        Scroll a profile / hashtag / related-videos page and save each video
        **immediately** as it is discovered — the CSV grows in real time.

        Does NOT wait until scrolling is finished before writing.
        """
        # --- resolve the page URL ------------------------------------------
        if entity_type == "user":
            page_url = f"{TIKTOK_BASE}/@{entity}"
        elif entity_type == "hashtag":
            page_url = f"{TIKTOK_BASE}/tag/{entity}"
        elif entity_type == "video_related":
            page_url = entity
        else:
            raise ValueError('entity_type must be "user", "hashtag", or "video_related"')

        drv = self._ensure_driver()
        drv.go(page_url, wait=3.0)

        target_user = entity.strip().lstrip("@").lower() if entity_type == "user" else ""

        def _is_allowed_href(href: str) -> bool:
            if "/video/" not in href:
                return False
            if entity_type != "user":
                return True
            match = re.search(r"/@([^/]+)/video/", href)
            if not match:
                return False
            return match.group(1).strip().lower() == target_user

        seen: set = set()          # every URL we have already saved
        saved: int = 0             # running count

        # --- helper: fetch + save one URL, skip if already seen ------------
        def _save_one(video_url: str):
            nonlocal saved
            if video_url in seen or saved >= count:
                return
            max_attempts = 2
            for attempt in range(1, max_attempts + 1):
                try:
                    self.save_tiktok(video_url, save_video=save_video,
                                     metadata_fn=metadata_fn, dir_path=dir_path)
                    seen.add(video_url)
                    saved += 1
                    logger.info("Saved %d/%d – %s", saved, count, video_url)
                    return
                except StaleElementReferenceException:
                    if attempt >= max_attempts:
                        logger.warning("Skipping stale URL after retries: %s", video_url)
                        return
                    time.sleep(random.uniform(0.2, 0.6))
                except Exception as exc:
                    logger.warning("Skipping URL due to error: %s | %s", video_url, exc)
                    return

        # --- initial JSON extraction (fast path) ---------------------------
        html = drv.page_source()
        tt_json = _extract_json_from_html(html)
        if tt_json and "__DEFAULT_SCOPE__" in tt_json:
            scope = tt_json["__DEFAULT_SCOPE__"]
            if entity_type == "user":
                for vid in scope.get("webapp.user-detail", {}).get("itemList", []):
                    if saved >= count:
                        break
                    _save_one(f"{TIKTOK_BASE}/@{entity}/video/{vid}")
            elif entity_type == "hashtag":
                for vid_id, item in tt_json.get("ItemModule", {}).items():
                    if saved >= count:
                        break
                    author = _safe(item, "author", "uniqueId", default="")
                    if author:
                        _save_one(f"{TIKTOK_BASE}/@{author}/video/{vid_id}")

        # --- scroll-and-save loop ------------------------------------------
        no_new = 0
        max_no_new = 30
        max_scrolls = max(300, count * 3)

        for _ in range(max_scrolls):
            if saved >= count:
                break

            prev_seen = len(seen)

            # grab every video link currently visible in the DOM
            try:
                hrefs = drv.driver.execute_script(
                    "return Array.from(document.querySelectorAll('a[href*=\"/video/\"]')).map(a => a.href || '');"
                ) or []
            except Exception:
                hrefs = []
                links = drv.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/video/"]')
                for link in links:
                    try:
                        href = link.get_attribute("href") or ""
                    except StaleElementReferenceException:
                        continue
                    hrefs.append(href)

            for href in hrefs:
                if saved >= count:
                    break
                if _is_allowed_href(href):
                    _save_one(href)

            if len(seen) == prev_seen:
                no_new += 1
                if no_new >= max_no_new:
                    logger.info("No new videos after %d scrolls – stopping.", max_no_new)
                    break
            else:
                no_new = 0

            drv.scroll_to_bottom()
            time.sleep(random.uniform(1.2, 2.0))

        logger.info("save_tiktok_multi_page done – %d video(s) saved.", saved)

    # =====================================================================
    # PUBLIC API  –  hidden_api paginated endpoints
    # =====================================================================
    # These only work when use_hidden_api=True.  They call TikTok's internal
    # JSON endpoints directly via _hidden_api_fetch().

    def _require_hidden_api(self):
        if not self.use_hidden_api:
            raise RuntimeError(
                "This method requires use_hidden_api=True. "
                "Re-create with Pyktok(use_hidden_api=True)."
            )

    # --- user detail ------------------------------------------------------
    def get_user_info(self, username: str) -> Optional[Dict]:
        """Return raw user-detail JSON (hidden_api mode)."""
        self._require_hidden_api()
        return self._hidden_api_fetch("user_detail", {"uniqueId": username, "secUid": ""})

    # --- user videos (paginated) ------------------------------------------
    def get_user_videos(self, username: str, count: int = 30) -> List[Dict]:
        """
        Return up to *count* video-object dicts for *username*.
        First call fetches user detail to get sec_uid, then paginates item_list.
        """
        self._require_hidden_api()

        # resolve sec_uid
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
        while len(videos) < count:
            resp = self._hidden_api_fetch("user_videos", {
                "secUid": sec_uid,
                "count": 35,
                "cursor": cursor,
            })
            if not resp:
                break
            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)

        return videos[:count]

    # --- hashtag videos (paginated) ---------------------------------------
    def get_hashtag_videos(self, hashtag: str, count: int = 30) -> List[Dict]:
        """Return up to *count* video dicts for a hashtag (hidden_api mode)."""
        self._require_hidden_api()

        # resolve challengeID
        detail = self._hidden_api_fetch("hashtag_detail", {"challengeName": hashtag})
        if not detail:
            return []
        challenge_id = _safe(detail, "challengeInfo", "challenge", "id", default="")
        if not challenge_id:
            logger.error("No challengeID for hashtag %s", hashtag)
            return []

        videos: List[Dict] = []
        cursor = 0
        while len(videos) < count:
            resp = self._hidden_api_fetch("hashtag_videos", {
                "challengeID": challenge_id,
                "count": 35,
                "cursor": cursor,
            })
            if not resp:
                break
            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)

        return videos[:count]

    # --- video comments (paginated) ---------------------------------------
    def get_video_comments(self, video_id: str, count: int = 30) -> List[Dict]:
        """Return up to *count* comment dicts for a video (hidden_api mode)."""
        self._require_hidden_api()
        # Strip to numeric ID if a full URL was passed
        m = re.search(VIDEO_ID_REGEX, str(video_id))
        vid = m.group(1) if m else str(video_id)

        comments: List[Dict] = []
        cursor = 0
        while len(comments) < count:
            resp = self._hidden_api_fetch("video_comments", {
                "aweme_id": vid,
                "count": 20,
                "cursor": cursor,
            })
            if not resp:
                break
            for c in resp.get("comments", []):
                comments.append(c)
                if len(comments) >= count:
                    break
            if not resp.get("has_more", False):
                break
            cursor = resp.get("cursor", 0)

        return comments[:count]

    # --- related videos ---------------------------------------------------
    def get_related_videos(self, video_id: str, count: int = 16) -> List[Dict]:
        """Return related-video dicts (hidden_api mode)."""
        self._require_hidden_api()
        m = re.search(VIDEO_ID_REGEX, str(video_id))
        vid = m.group(1) if m else str(video_id)

        resp = self._hidden_api_fetch("related_videos", {"itemID": vid, "count": count})
        if not resp:
            return []
        return resp.get("itemList", [])[:count]

    # --- trending / FYP ---------------------------------------------------
    def get_trending_videos(self, count: int = 30) -> List[Dict]:
        """Return trending video dicts (hidden_api mode)."""
        self._require_hidden_api()
        resp = self._hidden_api_fetch("trending", {"from_page": "fyp", "count": count})
        if not resp:
            return []
        return resp.get("itemList", [])[:count]

    # --- search users -----------------------------------------------------
    def search_users(self, keyword: str, count: int = 10) -> List[Dict]:
        """Search TikTok for users matching *keyword* (hidden_api mode)."""
        self._require_hidden_api()
        resp = self._hidden_api_fetch("search_users", {"keyword": keyword, "cursor": 0, "from_page": "search"})
        if not resp:
            return []
        return [u.get("user_info", {}) for u in resp.get("user_list", [])][:count]

    # --- sound ------------------------------------------------------------
    def get_sound_info(self, sound_id: str) -> Optional[Dict]:
        """Return sound-detail JSON (hidden_api mode)."""
        self._require_hidden_api()
        return self._hidden_api_fetch("sound_detail", {"musicId": sound_id})

    def get_sound_videos(self, sound_id: str, count: int = 30) -> List[Dict]:
        """Return videos using a particular sound (hidden_api mode)."""
        self._require_hidden_api()
        videos: List[Dict] = []
        cursor = 0
        while len(videos) < count:
            resp = self._hidden_api_fetch("sound_videos", {"musicID": sound_id, "count": 30, "cursor": cursor})
            if not resp:
                break
            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)
        return videos[:count]

    # --- playlist ---------------------------------------------------------
    def get_playlist_info(self, playlist_id: str) -> Optional[Dict]:
        """Return playlist-detail JSON (hidden_api mode)."""
        self._require_hidden_api()
        return self._hidden_api_fetch("playlist_detail", {"mixId": playlist_id})

    def get_playlist_videos(self, playlist_id: str, count: int = 30) -> List[Dict]:
        """Return videos in a playlist (hidden_api mode)."""
        self._require_hidden_api()
        videos: List[Dict] = []
        cursor = 0
        while len(videos) < count:
            resp = self._hidden_api_fetch("playlist_videos", {"mixId": playlist_id, "count": 30, "cursor": cursor})
            if not resp:
                break
            for item in resp.get("itemList", []):
                videos.append(item)
                if len(videos) >= count:
                    break
            if not resp.get("hasMore", False):
                break
            cursor = resp.get("cursor", 0)
        return videos[:count]

    # =====================================================================
    # Comments  –  requests / selenium mode (DOM-based fallback)
    # =====================================================================
    def get_comments(self, video_url: str, count: int = 30) -> pd.DataFrame:
        """
        Scrape comments.  If use_hidden_api=True, uses the JSON endpoint directly.
        Otherwise falls back to page-JSON extraction, then DOM scraping (selenium only).
        """
        # fast path: hidden_api
        if self.use_hidden_api:
            raw = self.get_video_comments(video_url, count)
            rows = []
            for c in raw:
                rows.append({
                    "cid":                  c.get("cid", ""),
                    "text":                 c.get("text", ""),
                    "user":                 _safe(c, "user", "uniqueId", default=""),
                    "digg_count":           c.get("digg_count", 0),
                    "reply_comment_total":  c.get("reply_comment_total", 0),
                    "create_time":          c.get("create_time", 0),
                })
            return pd.DataFrame(rows)

        # --- requests / selenium: page-JSON extraction --------------------
        html = self._get_html(video_url)
        tt_json = _extract_json_from_html(html)
        comments: List[Dict] = []

        if tt_json:
            vd = tt_json.get("__DEFAULT_SCOPE__", {}).get("webapp.video-detail", {})
            raw_comments = (
                vd.get("comments")
                or vd.get("commentList")
                or list(tt_json.get("CommentModule", {}).values())
                or []
            )
            for c in raw_comments[:count]:
                comments.append({
                    "cid":                  c.get("cid", ""),
                    "text":                 c.get("text", ""),
                    "user":                 _safe(c, "user", "uniqueId", default=""),
                    "digg_count":           c.get("digg_count", 0),
                    "reply_comment_total":  c.get("reply_comment_total", 0),
                    "create_time":          c.get("create_time", 0),
                })

        # --- DOM fallback (selenium only) --------------------------------
        if not comments and self.use_selenium:
            comments = self._scrape_comments_dom(video_url, count)

        if not comments:
            logger.warning("No comments extracted for %s", video_url)

        return pd.DataFrame(comments)

    def _scrape_comments_dom(self, video_url: str, count: int) -> List[Dict]:
        """Last-resort DOM scraping of comments (selenium mode)."""
        drv = self._ensure_driver()
        drv.go(video_url, wait=3.0)
        time.sleep(2)

        # Try to open the comment panel
        for sel in ['[data-e2e="comment-icon"]', '[data-e2e="browse-comment"]']:
            try:
                btns = drv.driver.find_elements(By.CSS_SELECTOR, sel)
                if btns:
                    btns[0].click()
                    time.sleep(2)
                    break
            except Exception:
                continue

        time.sleep(2)

        # Locate panel and scroll it
        panel = None
        for sel in ['[data-e2e="comment-panel"]', 'div[class*="comment-panel" i]']:
            try:
                els = drv.driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    panel = els[0]
                    break
            except Exception:
                continue

        if panel:
            for _ in range(min(20, count // 5 + 5)):
                try:
                    drv.driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollHeight;", panel
                    )
                    time.sleep(0.8)
                except Exception:
                    break

        # Collect comment elements
        candidates: List = []
        search_root = panel if panel else drv.driver
        for sel in ['div[class*="DivCommentContentWrapper"]',
                    'div[class*="DivCommentItemWrapper"]',
                    'span[data-e2e="comment-level-1"]']:
            try:
                found = search_root.find_elements(By.CSS_SELECTOR, sel)
                filtered = [e for e in found if len(e.text.strip()) > 3
                            and e.text.strip().lower() not in ("log in","sign up","comment","reply")]
                if filtered:
                    candidates = filtered
                    break
            except Exception:
                continue

        comments: List[Dict] = []
        for idx, elem in enumerate(candidates[:count]):
            try:
                full_text = elem.text.strip()
                if len(full_text) < 3:
                    continue

                # Try to pull username from an <a> inside
                username = ""
                try:
                    a = elem.find_element(By.CSS_SELECTOR, 'a[href*="@"]')
                    username = a.text.strip()
                except Exception:
                    pass

                # Strip trailing UI noise
                comment_text = full_text
                for noise in ("Reply", "Like", "More"):
                    if comment_text.endswith(noise):
                        comment_text = comment_text[: -len(noise)].strip()
                if username and comment_text.startswith(username):
                    comment_text = comment_text[len(username):].strip()

                if len(comment_text) > 2:
                    comments.append({
                        "cid":                  elem.get_attribute("data-cid") or f"dom_{idx}",
                        "text":                 comment_text,
                        "user":                 username,
                        "digg_count":           0,
                        "reply_comment_total":  0,
                        "create_time":          0,
                    })
            except Exception:
                continue

        return comments

    def save_comments(
        self,
        video_url: str,
        filename: str = "",
        count: int = 30,
    ) -> pd.DataFrame:
        """Get comments and optionally persist to CSV."""
        df = self.get_comments(video_url, count)
        if filename:
            combined = _deduplicate(filename, df, key="cid")
            combined.to_csv(filename, index=False)
            logger.info("Comments saved > %s (%d rows)", filename, len(combined))
        return df

    # =====================================================================
    # User profile data  (requests / selenium mode)
    # =====================================================================
    def get_user_data(self, username: str) -> Tuple[Dict, List[Dict]]:
        """
        Return (user_profile_dict, list_of_video_dicts) by scraping the profile page.
        Works in all modes; in hidden_api mode you can also use get_user_info() +
        get_user_videos() for richer paginated results.
        """
        url = f"{TIKTOK_BASE}/@{username}"
        html = self._get_html(url)
        tt_json = _extract_json_from_html(html)

        user_dict: Dict = {}
        videos: List[Dict] = []

        if tt_json:
            # --- new layout ---
            scope = tt_json.get("__DEFAULT_SCOPE__", {})
            ud = scope.get("webapp.user-detail", {})
            if ud:
                ui = ud.get("userInfo", {})
                user_data = ui.get("user", {})
                stats = ui.get("stats", {})
                user_dict = {**user_data, **{
                    "followerCount": stats.get("followerCount", 0),
                    "followingCount": stats.get("followingCount", 0),
                    "heartCount":     stats.get("heart", 0),
                    "videoCount":     stats.get("videoCount", 0),
                    "diggCount":      stats.get("diggCount", 0),
                }}

                item_list = ud.get("itemList", [])
                item_module = tt_json.get("ItemModule", {})
                for vid_id in item_list:
                    vid_str = str(vid_id)
                    if vid_str in item_module:
                        videos.append(item_module[vid_str])

            # --- legacy fallback ---
            if not user_dict:
                users = tt_json.get("UserModule", {}).get("users", {})
                user_dict = users.get(username) or (next(iter(users.values())) if users else {})

            if not videos:
                videos = list(tt_json.get("ItemModule", {}).values())[:30]

        return user_dict, videos

    # =====================================================================
    # File download helper
    # =====================================================================
    def _download_file(self, url: str, filename: str, dir_path: Optional[str] = None) -> str:
        path = os.path.join(dir_path, filename) if dir_path else filename
        if os.path.exists(path):
            logger.info("Already exists, skipping: %s", path)
            return path

        def _merged_cookie_dict() -> Dict[str, str]:
            merged: Dict[str, str] = {}

            # cookies from browser_cookie3 jar
            if self._cookies is not None:
                try:
                    for c in self._cookies:
                        if getattr(c, "name", None):
                            merged[str(c.name)] = str(c.value)
                except Exception:
                    pass

            # live cookies from Selenium session (often required for CDN video URLs)
            try:
                if self._driver and self._driver.driver:
                    for c in self._driver.driver.get_cookies():
                        name = c.get("name")
                        value = c.get("value")
                        if name and value is not None:
                            merged[str(name)] = str(value)
            except Exception:
                pass

            return merged

        base_headers = {**HEADERS, "referer": f"{TIKTOK_BASE}/", "origin": TIKTOK_BASE}
        attempts = [
            (base_headers, self._cookies),
            ({**base_headers, "accept": "*/*"}, _merged_cookie_dict()),
        ]

        last_exc: Optional[Exception] = None
        for idx, (hdrs, cookies) in enumerate(attempts, start=1):
            try:
                with requests.get(url, headers=hdrs, cookies=cookies, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                logger.info("Downloaded > %s", path)
                return path
            except Exception as exc:
                last_exc = exc
                if idx < len(attempts):
                    logger.warning("Download attempt %d failed, retrying with live session cookies: %s", idx, exc)

        # Remove partial file if any
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

        logger.error("Download failed for %s: %s", url, last_exc)
        return ""


# ===========================================================================
# Module-level singleton  –  one-liner API  (like the original pyktok)
# ===========================================================================
# Internal singleton instance; created / replaced by specify_browser().
_pyk: Optional[Pyktok] = None


def _get_pyk() -> Pyktok:
    """Return the active singleton, auto-creating a bare one if needed."""
    global _pyk
    if _pyk is None:
        _pyk = Pyktok()  # no cookies – user should call specify_browser first
        logger.warning(
            "No browser specified yet.  Call specify_browser('chrome') first "
            "for best results."
        )
    return _pyk


# ---------------------------------------------------------------------------
# specify_browser  –  initialise (or re-initialise) the module singleton
# ---------------------------------------------------------------------------
def specify_browser(
    browser: str = "chrome",
    *,
    use_selenium: bool = False,
    use_hidden_api: bool = False,
    download_videos: bool = False,
    headless: bool = True,
):
    """
    Initialise Pyktok with the cookies from *browser*.

    Call this once at the top of your script before using any other function.

    Parameters
    ----------
    browser         : "chrome" | "firefox" | "edge" | …
    use_selenium    : use undetected Chrome instead of plain HTTP
    use_hidden_api  : enable TikTok's internal JSON endpoints
    download_videos : default download flag for save_tiktok / save_tiktok_multi_*
    headless        : run browser headless (default True)
    """
    global _pyk
    if _pyk is not None:
        _pyk.close()          # shut down any previous browser session
    _pyk = Pyktok(
        use_selenium=use_selenium,
        use_hidden_api=use_hidden_api,
        download_videos=download_videos,
        browser_name=browser,
        headless=headless,
    )


# ---------------------------------------------------------------------------
# Single-video functions
# ---------------------------------------------------------------------------
def save_tiktok(
    video_url: str,
    save_video: bool = False,
    metadata_fn: str = "",
    dir_path: Optional[str] = None,
):
    """Download a single TikTok video and/or one metadata row.

    pyk.save_tiktok('https://www.tiktok.com/@tiktok/video/7106594312292453675',
                    True, 'video_data.csv')
    """
    _get_pyk().save_tiktok(video_url, save_video=save_video, metadata_fn=metadata_fn, dir_path=dir_path)


def get_tiktok_json(video_url: str) -> Optional[Dict[str, Any]]:
    """Return the full TikTok JSON object for a single video page.

    tt_json = pyk.get_tiktok_json('https://www.tiktok.com/@tiktok/video/7011536772089924869')
    """
    html = _get_pyk()._get_html(video_url)
    return _extract_json_from_html(html)


# ---------------------------------------------------------------------------
# Multi-URL  /  multi-page functions
# ---------------------------------------------------------------------------
def save_tiktok_multi_urls(
    video_urls: List[str],
    save_video: bool = False,
    metadata_fn: str = "",
    sleep: float = 4.0,
    dir_path: Optional[str] = None,
):
    """Download metadata (and optionally videos) for a list of URLs.

    pyk.save_tiktok_multi_urls([URL1, URL2], False, 'tiktok_data.csv', 1)
    """
    _get_pyk().save_tiktok_multi_urls(
        video_urls, save_video=save_video, metadata_fn=metadata_fn,
        sleep=sleep, dir_path=dir_path,
    )


def save_tiktok_multi_page(
    entity: str,
    ent_type: str = "user",
    count: int = 30,
    *,
    save_video: bool = False,
    metadata_fn: str = "",
    sleep: float = 4.0,
    dir_path: Optional[str] = None,
    headless: bool = True,
):
    """Scrape ~*count* videos from a user, hashtag, or related-videos page.

    ent_type : "user" | "hashtag" | "video_related"

    pyk.save_tiktok_multi_page('tiktok',    ent_type='user',          metadata_fn='tiktok.csv')
    pyk.save_tiktok_multi_page('datascience', ent_type='hashtag',     metadata_fn='ds.csv')
    pyk.save_tiktok_multi_page(VIDEO_URL,   ent_type='video_related', metadata_fn='rel.csv')
    """
    _get_pyk().save_tiktok_multi_page(
        entity, entity_type=ent_type, count=count,
        save_video=save_video, metadata_fn=metadata_fn,
        sleep=sleep, dir_path=dir_path,
    )


# ---------------------------------------------------------------------------
# Comment functions
# ---------------------------------------------------------------------------
def save_tiktok_comments(
    video_url: str,
    comment_count: int = 30,
    save_comments: bool = True,
    return_comments: bool = False,
    filename: str = "",
) -> Optional[pd.DataFrame]:
    """Download comments from a video, optionally saving to CSV.

    If *filename* is empty and *save_comments* is True, a default name is
    generated from the video ID.

    pyk.save_tiktok_comments(URL, comment_count=30, save_comments=True)
    """
    pyk = _get_pyk()
    df = pyk.get_comments(video_url, comment_count)

    if save_comments:
        if not filename:
            m = re.search(VIDEO_ID_REGEX, video_url)
            filename = f"comments_{m.group(1) if m else 'video'}.csv"
        pyk.save_comments(video_url, filename=filename, count=comment_count)

    return df if return_comments else None


def get_tiktok_comments(video_url: str, comment_count: int = 30) -> pd.DataFrame:
    """Return a DataFrame of comments for a video (does not write to disk).

    comments_df = pyk.get_tiktok_comments(URL, comment_count=30)
    """
    return _get_pyk().get_comments(video_url, comment_count)


# ---------------------------------------------------------------------------
# Hidden-API one-liners  (require specify_browser(..., use_hidden_api=True))
# ---------------------------------------------------------------------------
def get_user_info(username: str) -> Optional[Dict]:
    """Return raw user-detail JSON.

    info = pyk.get_user_info('therock')
    """
    return _get_pyk().get_user_info(username)


def get_user_videos(username: str, count: int = 30) -> List[Dict]:
    """Return up to *count* video dicts for a user (paginated).

    videos = pyk.get_user_videos('therock', count=50)
    """
    return _get_pyk().get_user_videos(username, count)


def get_hashtag_videos(hashtag: str, count: int = 30) -> List[Dict]:
    """Return up to *count* video dicts for a hashtag (paginated).

    vids = pyk.get_hashtag_videos('tylenol', count=20)
    """
    return _get_pyk().get_hashtag_videos(hashtag, count)


def get_video_comments(video_url: str, count: int = 30) -> List[Dict]:
    """Return up to *count* comment dicts via the hidden API (paginated).

    comments = pyk.get_video_comments(VIDEO_URL, count=40)
    """
    return _get_pyk().get_video_comments(video_url, count)


def get_related_videos(video_url: str, count: int = 16) -> List[Dict]:
    """Return related-video dicts via the hidden API.

    related = pyk.get_related_videos(VIDEO_URL, count=16)
    """
    return _get_pyk().get_related_videos(video_url, count)


def get_trending_videos(count: int = 30) -> List[Dict]:
    """Return trending / FYP video dicts.

    trending = pyk.get_trending_videos(count=30)
    """
    return _get_pyk().get_trending_videos(count)


def search_users(keyword: str, count: int = 10) -> List[Dict]:
    """Search TikTok for users matching *keyword*.

    users = pyk.search_users('science', count=10)
    """
    return _get_pyk().search_users(keyword, count)


def get_sound_info(sound_id: str) -> Optional[Dict]:
    """Return sound/music detail JSON.

    info = pyk.get_sound_info('7016547803243022337')
    """
    return _get_pyk().get_sound_info(sound_id)


def get_sound_videos(sound_id: str, count: int = 30) -> List[Dict]:
    """Return videos that use a particular sound (paginated).

    vids = pyk.get_sound_videos('7016547803243022337', count=20)
    """
    return _get_pyk().get_sound_videos(sound_id, count)


def get_playlist_info(playlist_id: str) -> Optional[Dict]:
    """Return playlist detail JSON.

    info = pyk.get_playlist_info('7426714779919797038')
    """
    return _get_pyk().get_playlist_info(playlist_id)


def get_playlist_videos(playlist_id: str, count: int = 30) -> List[Dict]:
    """Return videos inside a playlist (paginated).

    vids = pyk.get_playlist_videos('7426714779919797038', count=30)
    """
    return _get_pyk().get_playlist_videos(playlist_id, count)


# ---------------------------------------------------------------------------
# Cleanup helper
# ---------------------------------------------------------------------------
def close():
    """Shut down the browser session (if any).  Call when you are done."""
    global _pyk
    if _pyk:
        _pyk.close()
        _pyk = None


# ===========================================================================
# CLI entry-point
# ===========================================================================
def _cli():
    import argparse

    parser = argparse.ArgumentParser(
        description="TikTok scraper – boolean flags control the backend",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  # plain HTTP (fastest, default)
  python updated_pyktok_selenium_date.py user therock 50

  # Selenium fallback + download videos
  python updated_pyktok_selenium_date.py --selenium --download-videos user therock 30

  # Hidden-API mode (richest data, paginated)
  python updated_pyktok_selenium_date.py --hidden-api hashtag tylenol 20
  python updated_pyktok_selenium_date.py --hidden-api comments URL 40
        """,
    )
    # --- boolean flags ----------------------------------------------------
    parser.add_argument("--selenium",       action="store_true", help="Use Selenium instead of plain HTTP")
    parser.add_argument("--hidden-api",     action="store_true", help="Use TikTok hidden JSON endpoints")
    parser.add_argument("--download-videos",action="store_true", help="Download .mp4 / image files")

    # --- misc flags -------------------------------------------------------
    parser.add_argument("--browser",        default="chrome",     help="Browser for cookies (default chrome)")
    parser.add_argument("--no-headless",    action="store_true",  help="Show browser window")
    parser.add_argument("--output-dir",     default=None,         help="Directory for downloaded files")

    # --- positional -------------------------------------------------------
    parser.add_argument("command", choices=["user", "hashtag", "video", "comments"],
                        help="What to scrape")
    parser.add_argument("target", help="Username, hashtag, or video URL")
    parser.add_argument("count",  nargs="?", type=int, default=30,
                        help="Number of items (default 30)")

    args = parser.parse_args()

    with Pyktok(
        use_selenium    =args.selenium,
        use_hidden_api  =args.hidden_api,
        download_videos =args.download_videos,
        browser_name    =args.browser,
        headless        =not args.no_headless,
    ) as scraper:

        if args.command == "user":
            fn = f"{args.target}_videos.csv"
            if scraper.use_hidden_api:
                videos = scraper.get_user_videos(args.target, args.count)
                df = pd.DataFrame([generate_data_row(v).iloc[0] for v in videos])
                df.to_csv(fn, index=False)
                logger.info("Saved %d rows > %s", len(df), fn)
            else:
                scraper.save_tiktok_multi_page(
                    args.target, entity_type="user", count=args.count,
                    metadata_fn=fn, dir_path=args.output_dir,
                )

        elif args.command == "hashtag":
            fn = f"{args.target}_videos.csv"
            if scraper.use_hidden_api:
                videos = scraper.get_hashtag_videos(args.target, args.count)
                df = pd.DataFrame([generate_data_row(v).iloc[0] for v in videos])
                df.to_csv(fn, index=False)
                logger.info("Saved %d rows > %s", len(df), fn)
            else:
                scraper.save_tiktok_multi_page(
                    args.target, entity_type="hashtag", count=args.count,
                    metadata_fn=fn, dir_path=args.output_dir,
                )

        elif args.command == "video":
            scraper.save_tiktok(
                args.target,
                metadata_fn="video_metadata.csv",
                dir_path=args.output_dir,
            )

        elif args.command == "comments":
            df = scraper.save_comments(args.target, count=args.count)
            logger.info("Extracted %d comments.", len(df))


if __name__ == "__main__":
    _cli()