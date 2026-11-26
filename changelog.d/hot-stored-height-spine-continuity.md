---
section: Changed
---
- **A HOT index's stored heights and its outer spine are now specified and checked.** New spec
  requirement FMT-306 requires every indirect node's persisted height to equal the structural
  height below it — live in the writing transaction, in every published revision and after cold
  decode — and requires a structural change below a node to be carried back along the whole parent
  chain, so no reachable ancestor keeps describing the subtree it replaced. Locating a key still
  never depends on a stored height, so an inexact one cannot misroute: it is a malformed subtree,
  and the structural
  invariant validator now rejects it by comparing every walked node's height with the structure
  below it. A shape test and a lookup work budget pin the committed trie over eight full leaves at
  one indirect level and one routing step per sampled point lookup for both insertion orders. The repair
  this guards shipped earlier (issue
  [#1457](https://github.com/sirixdb/sirix/issues/1457)); height minimality remains out of scope
  under FMT-302.
