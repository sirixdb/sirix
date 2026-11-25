---
kind: row
---
| Non-strict path resolution stopped raising for symlink loops on a newer Python, allowing capture to guess a filesystem. | Require full resolution before assigning a mount; exercise missing targets, valid links and loops at leaf and parent components through capture output. Check documented runtime changes when a regression differs across interpreters. | [Capture path resolution](postmortems/benchmark-capture-path-resolution.md), `CaptureDetailTest` |
