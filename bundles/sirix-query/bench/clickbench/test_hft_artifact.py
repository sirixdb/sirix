#!/usr/bin/env python3
"""Behavior tests for normalized runtime-classpath identity."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from hft_artifact import runtime_classpath_sha256


class HftArtifactTest(unittest.TestCase):

    def test_dependency_content_and_relative_names_are_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            application = root / "application"
            dependency = root / "dependency.jar"
            application.mkdir()
            (application / "Main.class").write_bytes(b"main")
            dependency.write_bytes(b"dependency-v1")
            classpath = os.pathsep.join((str(application), str(dependency)))

            original = runtime_classpath_sha256(classpath)
            with self.assertRaisesRegex(ValueError, "runtime classpath entry is unreadable"):
                runtime_classpath_sha256(
                    os.pathsep.join((classpath, str(root / "missing-output")))
                )
            reordered = runtime_classpath_sha256(os.pathsep.join((str(dependency), str(application))))
            dependency.write_bytes(b"dependency-v2")
            changed_dependency = runtime_classpath_sha256(classpath)
            dependency.write_bytes(b"dependency-v1")
            (application / "Main.class").rename(application / "Renamed.class")
            changed_name = runtime_classpath_sha256(classpath)
            attestation = application / "META-INF" / "sirix-hft-build.properties"
            attestation.parent.mkdir()
            before_attestation = runtime_classpath_sha256(classpath)
            attestation.write_text("artifactSha256=stale\n", encoding="utf-8")

            self.assertNotEqual(original, reordered)
            self.assertNotEqual(original, changed_dependency)
            self.assertNotEqual(original, changed_name)
            self.assertEqual(before_attestation, runtime_classpath_sha256(classpath))


if __name__ == "__main__":
    unittest.main()
