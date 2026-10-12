#!/usr/bin/env python3
"""
stellaverse_rejected_filter.py

複数のテーブル (st, sl, sr, sn, so, dp, dpst) に対して
 - <table>/score_sub.json を取得し
 - 同じ <table> の score.json と score_vote.json に含まれる md5 を除外し
 - 加工後の JSON を <OUTPUT_DIR>/<table>/score.json に保存します

使い方:
  python stellaverse_rejected_filter.py                 # デフォルト出力先: <スクリプトディレクトリ>/docs
  python stellaverse_rejected_filter.py --output ./data

出力先例:
  ./docs/st/score.json
  ./docs/sl/score.json
  ./log/stellaverse_rejected_filter.log
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Set, Iterable

# テンプレートとテーブル一覧
TABLES = ("st", "sl", "sr", "sn", "so", "dp", "dpst")
SUB_URL_TEMPLATE = "https://stellabms.xyz/{table}/score_sub.json"
OFFICIAL_TEMPLATE = "https://stellabms.xyz/{table}/score.json"
VOTE_TEMPLATE = "https://stellabms.xyz/{table}/score_vote.json"

# スクリプトが置かれているディレクトリを基準に出力先を決定
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "docs")

LOG_DIRNAME = "log"
LOG_FILENAME = "stellaverse_rejected_filter.log"

# ネットワーク設定
FETCH_TIMEOUT = 20  # sec
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"


def setup_logging(output_dir: str) -> None:
    """ログ出力を設定する。ログファイルは output_dir/log/ に作られる。既存ファイルに追記する。"""
    os.makedirs(output_dir, exist_ok=True)
    log_dir = os.path.join(output_dir, LOG_DIRNAME)
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, LOG_FILENAME)

    # ハンドラを明示的に作って既存設定を上書き (複数回呼ばれても重複しないようにする)
    logger = logging.getLogger()
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    logger.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(sh)
    logger.addHandler(fh)


def fetch_json(url: str, timeout: int = FETCH_TIMEOUT, retries: int = RETRY_COUNT) -> List[Dict[str, Any]]:
    """譜面配列を取得・検証する。リトライ後も失敗した場合は例外を伝播する。"""
    if retries < 1:
        raise ValueError("取得回数は1以上で指定してください")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"ダウンロード: {url} (試行 {attempt}/{retries})")
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            if not isinstance(data, list):
                raise ValueError(f"{url}: JSONが配列ではありません")
            for i, item in enumerate(data):
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("md5"), str)
                    or not item["md5"].strip()
                ):
                    raise ValueError(f"{url}: 要素 {i} に有効なmd5文字列がありません")

            logging.info(f"取得成功: {url} -> {len(data)} 要素")
            return data
        except (urllib.error.URLError, ValueError, TimeoutError) as error:
            logging.warning(f"取得失敗: {url} ({error})")
            if attempt == retries:
                raise
            backoff = RETRY_BACKOFF ** attempt
            logging.info(f"{backoff:.1f}s 待機してリトライします...")
            time.sleep(backoff)


def extract_md5s(data: List[Dict[str, Any]]) -> Set[str]:
    """データ配列から md5 値の集合を抽出する。小文字正規化して返す。"""
    return {item["md5"].lower() for item in data}


def filter_sub(sub_data: List[Dict[str, Any]], exclude_md5s: Set[str]) -> List[Dict[str, Any]]:
    """sub_data の中から exclude_md5s に含まれる md5 を持つ要素を除外する。"""
    filtered: List[Dict[str, Any]] = []
    excluded_count = 0
    for item in sub_data:
        md5 = item["md5"]
        if md5.lower() in exclude_md5s:
            excluded_count += 1
            continue
        filtered.append(item)
    logging.info(f"フィルタ結果: 元 {len(sub_data)} 要素, 除外 {excluded_count} 要素, 残存 {len(filtered)} 要素")
    return filtered


def save_json(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"保存完了: {path} (合計 {len(data) if isinstance(data, list) else '?'} 要素)")


def process_table(table: str, output_dir: str) -> None:
    """必要な全データを検証してから保存する。取得・検証失敗時は既存の表を更新しない。"""
    logging.info(f"--- 処理開始: table={table} ---")
    sub_url = SUB_URL_TEMPLATE.format(table=table)
    official_url = OFFICIAL_TEMPLATE.format(table=table)
    vote_url = VOTE_TEMPLATE.format(table=table)

    sub = fetch_json(sub_url)
    official = fetch_json(official_url)
    if not official:
        raise ValueError(f"{table}: 正式表 (score.json) が0件のため更新できません")
    vote = fetch_json(vote_url)

    md5s_official = extract_md5s(official)
    md5s_vote = extract_md5s(vote)
    exclude_md5s = md5s_official.union(md5s_vote)

    logging.info(f"{table}: 除外対象 md5 数 = {len(exclude_md5s)}")

    filtered = filter_sub(sub, exclude_md5s)

    out_path = os.path.join(output_dir, table, "score.json")
    save_json(filtered, out_path)

    logging.info(f"--- 処理完了: table={table} ---")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="stellabms の JSON をテーブル単位でフィルタして保存します")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT_DIR, help="出力ディレクトリ (デフォルト: スクリプトディレクトリ/docs)")
    args = parser.parse_args(list(argv) if argv is not None else None)

    output_dir = args.output
    setup_logging(SCRIPT_DIR)

    logging.info("全テーブル処理を開始します")
    failed_tables = []
    for table in TABLES:
        try:
            process_table(table, output_dir)
        except Exception:
            failed_tables.append(table)
            logging.exception(f"{table} の更新に失敗したためスキップします。他のテーブルの処理は継続します。")

    if failed_tables:
        logging.error(f"更新に失敗したテーブル: {', '.join(failed_tables)}")
        return 1
    logging.info("全テーブル処理が終了しました")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
