import tempfile
import unittest
from pathlib import Path
import sys

TOOLS_PROJECT = Path(__file__).resolve().parents[1] / "tools" / "local_backfill_runner"
if str(TOOLS_PROJECT) not in sys.path:
    sys.path.insert(0, str(TOOLS_PROJECT))

from local_backfill_runner.backfill_runner import (  # noqa: E402
    BackfillStageError,
    build_image_index,
    parse_states_csv,
    resolve_local_image_path,
)


class LocalBackfillRunnerTests(unittest.TestCase):
    def test_parse_states_csv(self) -> None:
        self.assertEqual(parse_states_csv("done,failed"), ("done", "failed"))
        self.assertEqual(parse_states_csv(" done , failed , "), ("done", "failed"))

    def test_parse_states_csv_requires_values(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "states"):
            parse_states_csv(" , , ")

    def test_resolve_local_image_path_by_relative_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            target = root / "screenshots-v2" / "822" / "img.png"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"png")

            index = build_image_index(root)
            resolved = resolve_local_image_path("screenshots-v2/822/img.png", index)
            self.assertEqual(resolved, target)

    def test_resolve_local_image_path_by_basename(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            target = root / "img.png"
            target.write_bytes(b"png")

            index = build_image_index(root)
            resolved = resolve_local_image_path("screenshots-v2/822/img.png", index)
            self.assertEqual(resolved, target)

    def test_resolve_local_image_path_detects_ambiguous_basename(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "a").mkdir(parents=True, exist_ok=True)
            (root / "b").mkdir(parents=True, exist_ok=True)
            (root / "a" / "img.png").write_bytes(b"a")
            (root / "b" / "img.png").write_bytes(b"b")

            index = build_image_index(root)
            with self.assertRaisesRegex(BackfillStageError, "Ambiguous"):
                resolve_local_image_path("screenshots-v2/822/img.png", index)

    def test_resolve_local_image_path_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            index = build_image_index(Path(temp_dir))
            with self.assertRaisesRegex(BackfillStageError, "Missing local image"):
                resolve_local_image_path("screenshots-v2/822/img.png", index)


if __name__ == "__main__":
    unittest.main()
