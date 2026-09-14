# Dictionary-count v3: operational recovery

Firstmate inbox034 authorized exactly one maintenance-only execution of the existing immutable base-archive deduplication at a temporary 65 GiB floor, retaining all telemetry and no-swap 24 GiB scope protections. Inbox035 repeated that decision without authorizing a second execution. The recovery ran once, passed with two samples and changed no benchmark input or executable. The failed native and initial maintenance guard attempts remain untouched.

Immediate prechecks showed three distinct inodes, identical SHA-256 `beae4d882ded4273b3bb58897223b9b578a6e789189f4baccf9fc5e44ee8b828`, logical size 110,781,821 bytes, mode 664 and 216,400 allocated 512-byte blocks per copy. Postchecks show all three logical paths sharing inode 35815384 with unchanged bytes and modes. Two duplicate allocations of 110,796,800 bytes were removed; the recorded net filesystem free-space increase was 221,589,504 bytes. These hardlinked archives are immutable and must never be written in place.

Every subsequent preservation and audit phase used the unchanged 66 GiB ordinary floor:

- Frozen runtime jar deduplication verified 290 replacements and recorded a 332,779,520-byte net free-space increase.
- One additional byte-identical group-repeat base archive shares storage with the existing column-major-v5 base archive, preserving both paths and provenance.
- Six inactive instrumented/diagnostic images were gzipped and verified by exact decompression before removing their expanded copies. The recorded net free-space increase was 401,055,744 bytes. `dict-count-native-preservation.json` maps original paths and hashes to retained bytes; `restore-campaign-artifact.py` supports these entries. The accepted timed `pgo-group-repeat-100m/jb` executable and both 100M databases were untouched.
- The complete runtime preservation audit passed all 31 manifests and 38 shared-library entries with no failures. Root and frozen v3 production/test source hashes still match.

The fresh projected-budget audit reserves **959,531,492 bytes** for the remaining work: the prior instrumented build's 540,672,000-byte observed peak; two runtime copies; two training-profile copies; two optimized-image copies; 32 MiB for logs/attempts; and a 135,168,000-byte margin (25% of the observed native peak).

At audit time, 1,003,720,704 bytes were free above the ordinary guard floor. The projected minimum free space is 70,911,149,596 bytes, above the unchanged 70,866,960,384-byte floor. Conservative current candidate allocation is 3,097,464,832 bytes; adding the whole projected peak gives 4,056,996,324 bytes, below the original cumulative 4,294,967,296-byte allowance. Unique-inode allocation is lower, but the retry gate retains conservative accounting.

`dict-count-v3-recovery-budget.json` contains the component calculation, exact archive checks, guard results and allocation inventory. The retry driver checks live free space against that full estimate again immediately before starting. It uses a new instrument-build attempt prefix and `pgo-dict-count-v3-retry1-100m/`, preserving the failed attempt and original empty output directory. All ordinary guards remain active throughout the fresh PGO and full-size comparisons.

Only the operational disk-floor key is resolved. The independent column-major capacity decision remains open; no new 100M database, baseline deletion, shared-service change, reset or handoff was performed. Rank one remains unproven.
