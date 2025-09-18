#!/usr/bin/env python3
"""
stellaverse_rejected_filter.py

複数のテーブル (st, sl, sn, so, fr, dp, dpst 等) に対して
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
import urllib.request
import urllib.error
from typing import Any, Dict, List, Set, Iterable

# テンプレートとテーブル一覧
TABLES = ("st", "sl", "sn", "so", "dp", "dpst")
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


def fetch_json(url: str, timeout: int = FETCH_TIMEOUT, retries: int = RETRY_COUNT) -> Any:
    """URLからJSONを取得してパースして返す。例外は呼び出し元へ伝播する。"""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"ダウンロード: {url} (試行 {attempt}/{retries})")
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                raw = resp.read()
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    text = raw.decode("latin-1")
                data = json.loads(text)
                length = getattr(data, '__len__', lambda: '?')()
                logging.info(f"取得成功: {url} -> {length} 要素")
                return data
        except (urllib.error.HTTPError, urllib.error.URLError, ValueError, TimeoutError) as e:
            last_err = e
            logging.warning(f"取得失敗: {url} ({e})")
            if attempt < retries:
                backoff = RETRY_BACKOFF ** attempt
                logging.info(f"{backoff:.1f}s 待機してリトライします...")
                time.sleep(backoff)
    logging.error(f"最終的に取得できませんでした: {url}")
    raise last_err


def extract_md5s(data: Any) -> Set[str]:
    """データ配列から md5 値の集合を抽出する。小文字正規化して返す。"""
    md5s: Set[str] = set()
    if not isinstance(data, list):
        logging.warning("期待した形式ではありません: list ではありません")
        return md5s
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            continue
        md5 = item.get("md5")
        if not md5:
            continue
        md5s.add(md5.lower())
    return md5s


def filter_sub(sub_data: List[Dict[str, Any]], exclude_md5s: Set[str]) -> List[Dict[str, Any]]:
    """sub_data の中から exclude_md5s に含まれる md5 を持つ要素を除外する。"""
    filtered: List[Dict[str, Any]] = []
    excluded_count = 0
    for item in sub_data:
        if not isinstance(item, dict):
            filtered.append(item)
            continue
        md5 = item.get("md5")
        if not md5:
            filtered.append(item)
            continue
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
    """1つの table を処理する。失敗しても他の table の処理を続ける。"""
    logging.info(f"--- 処理開始: table={table} ---")
    sub_url = SUB_URL_TEMPLATE.format(table=table)
    official_url = OFFICIAL_TEMPLATE.format(table=table)
    vote_url = VOTE_TEMPLATE.format(table=table)

    # sub は必須。取得失敗ならこの table はスキップ
    try:
        sub = fetch_json(sub_url)
    except Exception:
        logging.exception(f"{table}: score_sub の取得に失敗したためスキップします。")
        return

    # official / vote は存在しないことがあり得るので、失敗しても空リスト扱いして続行
    try:
        official = fetch_json(official_url)
    except Exception:
        logging.warning(f"{table}: official (score.json) の取得に失敗しました。除外リストは空として処理します。")
        official = []

    try:
        vote = fetch_json(vote_url)
    except Exception:
        logging.warning(f"{table}: vote (score_vote.json) の取得に失敗しました。除外リストは空として処理します。")
        vote = []

    md5s_official = extract_md5s(official)
    md5s_vote = extract_md5s(vote)
    exclude_md5s = md5s_official.union(md5s_vote)

    logging.info(f"{table}: 除外対象 md5 数 = {len(exclude_md5s)}")

    if not isinstance(sub, list):
        logging.error(f"{table}: score_sub の形式が配列ではありません。スキップします。")
        return

    filtered = filter_sub(sub, exclude_md5s)

    out_path = os.path.join(output_dir, table, "score.json")
    save_json(filtered, out_path)

    logging.info(f"--- 処理完了: table={table} ---")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="stellabms の JSON をテーブル単位でフィルタして保存します")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT_DIR, help="出力ディレクトリ (デフォルト: スクリプトディレクトリ/output)")
    args = parser.parse_args(list(argv) if argv is not None else None)

    output_dir = args.output
    setup_logging(SCRIPT_DIR)

    logging.info("全テーブル処理を開始します")
    for table in TABLES:
        try:
            process_table(table, output_dir)
        except Exception:
            logging.exception(f"例外により {table} の処理が中断されましたが、他のテーブルの処理は継続します。")

    logging.info("全テーブル処理が終了しました")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
