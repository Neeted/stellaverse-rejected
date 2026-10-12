"""取得失敗時の更新抑止と、提案落ちの集合演算を検証する。"""

import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import call, patch

import stellaverse_rejected_filter as target


def response(data):
    return io.BytesIO(json.dumps(data).encode("utf-8"))


class FetchTests(unittest.TestCase):
    def setUp(self):
        sleep_patch = patch.object(target.time, "sleep")
        self.sleep = sleep_patch.start()
        self.addCleanup(sleep_patch.stop)
        warning_patch = patch.object(target.logging, "warning")
        warning_patch.start()
        self.addCleanup(warning_patch.stop)

    def test_browser_user_agent_and_timeout(self):
        with patch.object(target.urllib.request, "urlopen", return_value=response([])) as urlopen:
            self.assertEqual(target.fetch_json("https://example.com/score.json"), [])
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("User-agent"), target.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], target.FETCH_TIMEOUT)
        self.sleep.assert_not_called()

    def test_request_failures_retry_three_times_then_raise(self):
        for error in (urllib.error.HTTPError("https://example.com", 403, "Forbidden", {}, None),
                      urllib.error.URLError("接続失敗"), TimeoutError("タイムアウト")):
            with self.subTest(error=error):
                self.sleep.reset_mock()
                with patch.object(target.urllib.request, "urlopen", side_effect=error) as urlopen:
                    with self.assertRaises(type(error)):
                        target.fetch_json("https://example.com")
                self.assertEqual(urlopen.call_count, 3)
                self.assertEqual(self.sleep.call_args_list, [call(1.5), call(2.25)])

    def test_retry_can_recover_with_browser_user_agent(self):
        error = urllib.error.HTTPError("https://example.com", 403, "Forbidden", {}, None)
        with patch.object(target.urllib.request, "urlopen", side_effect=[error, response([])]) as urlopen:
            self.assertEqual(target.fetch_json("https://example.com"), [])
        self.assertEqual(urlopen.call_count, 2)
        self.sleep.assert_called_once_with(1.5)
        for attempt in urlopen.call_args_list:
            self.assertEqual(attempt.args[0].get_header("User-agent"), target.USER_AGENT)

    def test_invalid_json_can_recover_on_retry(self):
        with patch.object(target.urllib.request, "urlopen", side_effect=[
            io.BytesIO(b"invalid JSON"), response([{"md5": "abc"}])
        ]):
            self.assertEqual(target.fetch_json("https://example.com"), [{"md5": "abc"}])
        self.sleep.assert_called_once_with(1.5)

    def test_invalid_payload_is_rejected(self):
        for payload in (b"<html>Forbidden</html>", b"\xff", b"{}", b"null", b"[null]",
                        b"[{}]", b'[{"md5": null}]', b'[{"md5": 1}]', b'[{"md5": " "}]'):
            with self.subTest(payload=payload):
                self.sleep.reset_mock()
                with patch.object(target.urllib.request, "urlopen", side_effect=lambda *args, **kwargs: io.BytesIO(payload)) as urlopen:
                    with self.assertRaises(ValueError):
                        target.fetch_json("https://example.com")
                self.assertEqual(urlopen.call_count, 3)
                self.assertEqual(self.sleep.call_args_list, [call(1.5), call(2.25)])


class ProcessTests(unittest.TestCase):
    def setUp(self):
        sleep_patch = patch.object(target.time, "sleep")
        sleep_patch.start()
        self.addCleanup(sleep_patch.stop)
        warning_patch = patch.object(target.logging, "warning")
        warning_patch.start()
        self.addCleanup(warning_patch.stop)

    def test_official_and_vote_are_excluded_case_insensitively(self):
        sub = [{"md5": "ABC", "title": "正式"}, {"md5": "DEF", "title": "投票中"},
               {"md5": "ghi", "title": "提案落ち", "level": "1"}]
        with tempfile.TemporaryDirectory() as output:
            with patch.object(target.urllib.request, "urlopen", side_effect=[
                response(sub), response([{"md5": "abc"}]), response([{"md5": "def"}])
            ]):
                target.process_table("sr", output)
            saved = json.loads((Path(output) / "sr" / "score.json").read_text(encoding="utf-8"))
            self.assertEqual(saved, [sub[2]])

    def test_empty_vote_and_empty_filtered_result_are_valid(self):
        with tempfile.TemporaryDirectory() as output:
            with patch.object(target.urllib.request, "urlopen", side_effect=[
                response([{"md5": "abc"}]), response([{"md5": "ABC"}]), response([])
            ]):
                target.process_table("sr", output)
            self.assertEqual(json.loads((Path(output) / "sr" / "score.json").read_text()), [])

    def test_failed_inputs_preserve_existing_output(self):
        valid = [{"md5": "abc"}]
        for input_index in range(3):
            for failure in ("http", "json", "shape", "md5", "empty_official"):
                if failure == "empty_official" and input_index != 1:
                    continue
                with self.subTest(input_index=input_index, failure=failure):
                    inputs = [valid, valid, []]
                    if failure == "http":
                        inputs[input_index] = urllib.error.HTTPError("https://example.com", 403, "Forbidden", {}, None)
                    elif failure == "json":
                        inputs[input_index] = b"invalid JSON"
                    elif failure == "shape":
                        inputs[input_index] = {}
                    elif failure == "md5":
                        inputs[input_index] = [{}]
                    else:
                        inputs[input_index] = []

                    def open_input(request, **kwargs):
                        filename = request.full_url.rsplit("/", 1)[1]
                        data = inputs[("score_sub.json", "score.json", "score_vote.json").index(filename)]
                        if isinstance(data, Exception):
                            raise data
                        if isinstance(data, bytes):
                            return io.BytesIO(data)
                        return response(data)

                    with tempfile.TemporaryDirectory() as output:
                        path = Path(output) / "sr" / "score.json"
                        path.parent.mkdir()
                        original = b'[{"md5": "old", "title": "existing"}]'
                        path.write_bytes(original)
                        with patch.object(target.urllib.request, "urlopen", side_effect=open_input):
                            with self.assertRaises((urllib.error.HTTPError, ValueError)):
                                target.process_table("sr", output)
                        self.assertEqual(path.read_bytes(), original)


class MainTests(unittest.TestCase):
    def test_six_successful_tables_are_updated_and_failed_table_is_preserved(self):
        def fetch(url):
            if url == target.OFFICIAL_TEMPLATE.format(table="sl"):
                raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)
            if url.endswith("score_sub.json"):
                return [{"md5": "rejected"}, {"md5": "official"}]
            if url.endswith("score_vote.json"):
                return []
            return [{"md5": "official"}]

        with tempfile.TemporaryDirectory() as output:
            for table in target.TABLES:
                path = Path(output) / table / "score.json"
                path.parent.mkdir()
                path.write_text('[{"md5": "old"}]', encoding="utf-8")
            with patch.object(target, "setup_logging"), patch.object(target, "fetch_json", side_effect=fetch):
                with self.assertLogs(level="ERROR"):
                    self.assertEqual(target.main(["--output", output]), 1)
            for table in target.TABLES:
                with self.subTest(table=table):
                    saved = json.loads((Path(output) / table / "score.json").read_text(encoding="utf-8"))
                    self.assertEqual(saved, [{"md5": "old" if table == "sl" else "rejected"}])

    def test_success_returns_zero(self):
        with patch.object(target, "setup_logging"), patch.object(target, "process_table"):
            self.assertEqual(target.main([]), 0)

    def test_starlight_follows_satellite_and_fr_is_not_updated(self):
        self.assertEqual(target.TABLES[target.TABLES.index("sl") + 1], "sr")
        self.assertNotIn("fr", target.TABLES)


if __name__ == "__main__":
    unittest.main()
