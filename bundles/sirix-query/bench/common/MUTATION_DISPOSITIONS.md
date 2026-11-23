# Mutation dispositions of the benchmark protocol tools

Mutation record for [`timed_gate.py`](timed_gate.py), [`bench_trend.py`](bench_trend.py) and
[`bench_ledger.py`](bench_ledger.py), kept by the rules of the Java lane
([CONV-103](../../../../spec/CONV-conventions.md#conv-103--specification-before-implementation),
[CONV-201](../../../../spec/CONV-conventions.md#conv-201--mutation-disposition-record)): every
surviving mutant is killed by a behaviour test derived from CONV-302 to CONV-304, removed together
with code that had no observable effect, or listed below as equivalent with its reason. No test was
written to kill a mutant for its own sake.

## How the run was made

Each module was mutated one change at a time, and its own test module was run against every mutant
(`python3 -B -m unittest -f test_<module>`). A mutant is killed when that run fails. The mutations,
applied at every site of the module's syntax tree:

| Operator | Change |
|---|---|
| comparison | `<` ↔ `<=`, `>` ↔ `>=`, `==` ↔ `!=`, `in` ↔ `not in`, `is` ↔ `is not` |
| arithmetic | `+` ↔ `-`, `*` ↔ `/`, `//` → `*`, `%` → `*`, `<<` ↔ `>>` |
| boolean | `and` ↔ `or`, `not x` → `x` |
| constant | a number *n* → *n* + 1, `True` ↔ `False` |
| control | an `if` condition → `False`, `raise` → `pass`, `continue` → `pass`, `break` → `pass` |

String constants are not mutated, so the wording of messages is outside this record. The driver was
a short script over Python's `ast` module and is not part of the repository; any mutation tool with
these operators reproduces the sites. `test_kit_recording.py` took no part in the runs: it drives
the kit scripts and reaches the modules only through their command lines.

| Module | Mutants | Killed | Equivalent |
|---|---:|---:|---:|
| `timed_gate.py` | 247 | 240 | 7 |
| `bench_trend.py` | 112 | 108 | 4 |
| `bench_ledger.py` | 581 | 576 | 5 |

The first runs left 34 survivors in `timed_gate.py`, 22 in `bench_trend.py` and 113 in
`bench_ledger.py`. They were answered in three ways.

- **Tests of a requirement that had none.** Among them: the fence factor of CONV-304; levels
  compared as ratios whatever their size; the strict bounds of the drift rule and the inclusive bound
  of the stop rule, at floating-point inputs that reach them exactly; rejected allocation legs and
  legs outside the judged set; the counts in the report; the rejection of a whole run; a ledger
  never created by an empty append; a short write; the options every command requires; the exit
  status of the scripts.
- **A defect.** A benchmark data path that cannot be resolved, such as a symbolic-link loop, raised
  out of the capture instead of being recorded as unavailable. It is fixed and tested.
- **Code removed.** Conditions and options that changed nothing: a second check of an arm's name, a
  loop guard that could not be reached, a default that was never used, key sorting and character
  escaping options of files that are only ever read back as JSON.

## `timed_gate.py`: 7 equivalent

| Line | Mutation | Why it cannot be observed |
|---|---|---|
| `split = count // 2` | `2` → `3` | The split only decides how the residuals are divided into two groups whose signed sums are then combined. Every division yields the same 2^(n−1) absolute sums. |
| `high = [total + residuals[-1] ...]` | `+` → `-` | Fixing the last sign to minus instead of plus negates every sum, and only absolute values are used. |
| `values = [abs(left + right) ...]` | `+` → `-` | The signed sums of the low group are closed under negation, so `left - right` ranges over the same values as `left + right`. |
| `@functools.lru_cache(maxsize=512)` | `512` → `513` | The size of a cache in front of a pure function. |
| `@dataclass(frozen=True)` on `Series`, `StopState`, `Judgement` | `True` → `False`, three sites | Immutability of result objects guards against misuse inside the module. No requirement observes it. |

## `bench_trend.py`: 4 equivalent

| Line | Mutation | Why it cannot be observed |
|---|---|---|
| `if nights[0].night < night:` | `<` → `<=` | The branch is reached only when the series has no entry for `night`, so its first night never equals it. |
| `@dataclass(frozen=True)` on `Night`, `NightState`, `TrendReport` | `True` → `False`, three sites | As above. |

## `bench_ledger.py`: 5 equivalent

| Line | Mutation | Why it cannot be observed |
|---|---|---|
| `subprocess.run(..., timeout=20, ...)` | `20` → `21` | The patience with a probe command; no probe in a test runs that long. |
| `os.open(path, ..., 0o644)` | `0o644` → `0o645` | One permission bit of a newly created ledger file; no requirement states its mode. |
| `if existing.read(1) != b"\n":` | `1` → `2` | The read starts at the last byte of the file, so asking for two bytes returns the same one. |
| `json.dumps(document, indent=2)` in `_write_json` | `2` → `3` | The indentation of `run.json` and `environment.json`, which are read back as JSON. |
| `json.dumps(fingerprint_fields(environment), indent=2)` | `2` → `3` | The indentation of the fields `bench_ledger.py fingerprint` prints. |
