# Mutation dispositions of the PR mutation reporter (mutmut)

Self-mutation record for [`pr_mutation.py`](../pr_mutation.py), kept by the same rules as
the Java lane ([CONV-103](../../../spec/CONV-conventions.md#conv-103--specification-before-implementation),
[CONV-201](../../../spec/CONV-conventions.md#conv-201--mutation-disposition-record)): every
surviving mutant on a line this change wrote is killed by a behaviour test, removed with
code that had no observable effect, or listed below as equivalent with its reason. Tests
were never written to kill a mutant for its own sake.

Run: mutmut 3.8.1 on commit `90e275f5c5c714cef13bb308c60457509fc630b2` against `test_pr_mutation*.py` (`source_paths =
pr_mutation.py`). Totals for the whole script: 3826 mutants, 3388 killed, 438 survived, 0 without a covering test.
170 survivors sit on lines this change wrote and are listed here; the other 268 sit on lines it did not change and are advisory debt, as
in the Java lane. Mutant names are mutmut's and are valid for the commit above only.

Reproduce: copy `pr_mutation.py` and `tests/` into a scratch project with
`[mutmut] source_paths = pr_mutation.py`, place `.github/` and `bundles/` two levels
above the copied script (the tests read the repository files from there), and run
`mutmut run`.

## equivalent (wording): 142

CONV-202 and CONV-203 fix what this text must carry (entry position, violated rule, resolving action, counts, kind, helper command); headings, help texts and helper output layout are not specified. The mutant changes only wording, letter case, blank lines or adds an `XX` decoration, keeping every specified element, so no specified behaviour differs.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_read_report__mutmut_54` | 465 | changed['failing'] = [{'mutant': value, 'reason': 'no disposition entry'} | changed['failing'] = [{'mutant': value, 'reason': 'XXno disposition entryXX'} |
| `x_read_report__mutmut_55` | 465 | changed['failing'] = [{'mutant': value, 'reason': 'no disposition entry'} | changed['failing'] = [{'mutant': value, 'reason': 'NO DISPOSITION ENTRY'} |
| `x_evidence_problems__mutmut_2` | 567 | return [''evidence' must be a table'] | return ['XX'evidence' must be a tableXX'] |
| `x_evidence_problems__mutmut_8` | 570 | problems.append(''evidence' must have exactly the fields ' | problems.append('XX'evidence' must have exactly the fields XX' |
| `x_evidence_problems__mutmut_11` | 571 | + ', '.join(sorted(EVIDENCE_FIELDS))) | + 'XX, XX'.join(sorted(EVIDENCE_FIELDS))) |
| `x_evidence_problems__mutmut_38` | 583 | problems.append(''evidence.command' must be a non-empty list of non-empty strings') | problems.append('XX'evidence.command' must be a non-empty list of non-empty stringsXX') |
| `x_evidence_problems__mutmut_50` | 585 | problems.append(''evidence.baseline_exit' must be 0: the test passes without the mutant') | problems.append('XX'evidence.baseline_exit' must be 0: the test passes without the mutantXX') |
| `x_evidence_problems__mutmut_62` | 587 | problems.append(''evidence.injected_exit' must be a non-zero integer: the mutant fails the test') | problems.append('XX'evidence.injected_exit' must be a non-zero integer: the mutant fails the testXX') |
| `x_evidence_problems__mutmut_79` | 591 | problems.append(''evidence.sources' must be a non-empty array of tables') | problems.append('XX'evidence.sources' must be a non-empty array of tablesXX') |
| `x_evidence_problems__mutmut_99` | 598 | problems.append('each 'evidence.sources' entry needs exactly 'path' and a lowercase ' | problems.append('XXeach 'evidence.sources' entry needs exactly 'path' and a lowercase XX' |
| `x_evidence_problems__mutmut_101` | 599 | 'hexadecimal SHA-256 'sha256'') | 'XXhexadecimal SHA-256 'sha256'XX') |
| `x_evidence_problems__mutmut_102` | 599 | 'hexadecimal SHA-256 'sha256'') | 'hexadecimal sha-256 'sha256'') |
| `x_evidence_problems__mutmut_103` | 599 | 'hexadecimal SHA-256 'sha256'') | 'HEXADECIMAL SHA-256 'SHA256'') |
| `x_evidence_problems__mutmut_116` | 607 | problems.append(''evidence.sources' must hash each file the patch touches exactly once') | problems.append('XX'evidence.sources' must hash each file the patch touches exactly onceXX') |
| `x_entry_problems__mutmut_7` | 615 | return [f''kind' must be one of {", ".join(KINDS)}, not {kind!r}'] | return [f''kind' must be one of {"XX, XX".join(KINDS)}, not {kind!r}'] |
| `x_entry_problems__mutmut_17` | 619 | problems.append('missing field(s): ' + ', '.join(sorted(missing))) | problems.append('missing field(s): ' + 'XX, XX'.join(sorted(missing))) |
| `x_entry_problems__mutmut_24` | 621 | problems.append('unknown field(s): ' + ', '.join(sorted(unknown))) | problems.append('unknown field(s): ' + 'XX, XX'.join(sorted(unknown))) |
| `x_entry_problems__mutmut_66` | 633 | problems.append(''description' must be exact text without pattern or control characters') | problems.append('XX'description' must be exact text without pattern or control charactersXX') |
| `x_entry_problems__mutmut_82` | 637 | problems.append(''indexes' must be a non-empty list of non-negative integers') | problems.append('XX'indexes' must be a non-empty list of non-negative integersXX') |
| `x_entry_problems__mutmut_103` | 640 | problems.append(''checked_at' must be a full lowercase hexadecimal commit ID') | problems.append('XX'checked_at' must be a full lowercase hexadecimal commit IDXX') |
| `x_entry_problems__mutmut_104` | 640 | problems.append(''checked_at' must be a full lowercase hexadecimal commit ID') | problems.append(''checked_at' must be a full lowercase hexadecimal commit id') |
| `x_entry_problems__mutmut_139` | 652 | problems.append(''test' must be '<binary test class name>#<method name>'') | problems.append('XX'test' must be '<binary test class name>#<method name>'XX') |
| `x_parse_dispositions__mutmut_12` | 667 | + ', '.join(sorted(document.keys() - {'mutant'}))] | + ', '.join(sorted(document.keys() - {'XXmutantXX'}))] |
| `x_parse_dispositions__mutmut_13` | 667 | + ', '.join(sorted(document.keys() - {'mutant'}))] | + ', '.join(sorted(document.keys() - {'MUTANT'}))] |
| `x_parse_dispositions__mutmut_9` | 667 | + ', '.join(sorted(document.keys() - {'mutant'}))] | + 'XX, XX'.join(sorted(document.keys() - {'mutant'}))] |
| `x_load_associations__mutmut_27` | 749 | errors.append(f'association {name}: {", ".join(absent)} not declared in the tests of ' | errors.append(f'association {name}: {"XX, XX".join(absent)} not declared in the tests of ' |
| `x_judge_entry__mutmut_60` | 847 | return 'superseded', 'an in-lane test now detects this mutant; delete the entry' | return 'superseded', 'XXan in-lane test now detects this mutant; delete the entryXX' |
| `x_judge_entry__mutmut_61` | 847 | return 'superseded', 'an in-lane test now detects this mutant; delete the entry' | return 'superseded', 'AN IN-LANE TEST NOW DETECTS THIS MUTANT; DELETE THE ENTRY' |
| `x_judge_entry__mutmut_91` | 853 | + (' and re-run the counterfactual' if entry['kind'] == 'killed-out-of-lane' else '') | + ('XX and re-run the counterfactualXX' if entry['kind'] == 'killed-out-of-lane' else '') |
| `x_judge_entry__mutmut_98` | 853 | + (' and re-run the counterfactual' if entry['kind'] == 'killed-out-of-lane' else '') | + (' and re-run the counterfactual' if entry['kind'] == 'killed-out-of-lane' else 'XXXX') |
| `x_judge_entry__mutmut_99` | 854 | + ', then update the entry, at least 'checked_at'' | + 'XX, then update the entry, at least 'checked_at'XX' |
| `x_judge_entry__mutmut_103` | 855 | + ('' if mutant else '; no reported mutant matches it any more, so ' + REMAP_HINT)) | + ('XXXX' if mutant else '; no reported mutant matches it any more, so ' + REMAP_HINT)) |
| `x_judge_entry__mutmut_105` | 855 | + ('' if mutant else '; no reported mutant matches it any more, so ' + REMAP_HINT)) | + ('' if mutant else 'XX; no reported mutant matches it any more, so XX' + REMAP_HINT)) |
| `x_judge_entry__mutmut_106` | 855 | + ('' if mutant else '; no reported mutant matches it any more, so ' + REMAP_HINT)) | + ('' if mutant else '; NO REPORTED MUTANT MATCHES IT ANY MORE, SO ' + REMAP_HINT)) |
| `x_judge_entry__mutmut_132` | 862 | return 'advisory', 'disposes of an unchanged-line survivor; advisory only' | return 'advisory', 'XXdisposes of an unchanged-line survivor; advisory onlyXX' |
| `x_judge_entry__mutmut_133` | 862 | return 'advisory', 'disposes of an unchanged-line survivor; advisory only' | return 'advisory', 'DISPOSES OF AN UNCHANGED-LINE SURVIVOR; ADVISORY ONLY' |
| `x_judge_entry__mutmut_153` | 872 | where = ("not in this run's sample of its class" if complete is not None | where = ("XXnot in this run's sample of its classXX" if complete is not None |
| `x_judge_entry__mutmut_156` | 873 | else 'its class was not mutated in this run') | else 'XXits class was not mutated in this runXX') |
| `x_judge_entry__mutmut_167` | 879 | return 'not judged', 'no reported mutant matches; its class was not mutated in this run' | return 'not judged', 'XXno reported mutant matches; its class was not mutated in this runXX' |
| `x_judge_entry__mutmut_171` | 880 | return 'out of scope', 'class not mutated in this run' | return 'out of scope', 'XXclass not mutated in this runXX' |
| `x_judge_entry__mutmut_172` | 880 | return 'out of scope', 'class not mutated in this run' | return 'out of scope', 'CLASS NOT MUTATED IN THIS RUN' |
| `x_apply_dispositions__mutmut_79` | 938 | reason = ('NO_COVERAGE is never dispositioned: test the specified behaviour that ' | reason = ('XXNO_COVERAGE is never dispositioned: test the specified behaviour that XX' |
| `x_apply_dispositions__mutmut_80` | 938 | reason = ('NO_COVERAGE is never dispositioned: test the specified behaviour that ' | reason = ('no_coverage is never dispositioned: test the specified behaviour that ' |
| `x_apply_dispositions__mutmut_82` | 939 | 'needs this code, remove code with no specified behaviour, or associate ' | 'XXneeds this code, remove code with no specified behaviour, or associate XX' |
| `x_apply_dispositions__mutmut_83` | 939 | 'needs this code, remove code with no specified behaviour, or associate ' | 'NEEDS THIS CODE, REMOVE CODE WITH NO SPECIFIED BEHAVIOUR, OR ASSOCIATE ' |
| `x_apply_dispositions__mutmut_84` | 940 | 'the existing tests that cover it (CONV-202, CONV-204)') | 'XXthe existing tests that cover it (CONV-202, CONV-204)XX') |
| `x_apply_dispositions__mutmut_85` | 940 | 'the existing tests that cover it (CONV-202, CONV-204)') | 'the existing tests that cover it (conv-202, conv-204)') |
| `x_apply_dispositions__mutmut_86` | 940 | 'the existing tests that cover it (CONV-202, CONV-204)') | 'THE EXISTING TESTS THAT COVER IT (CONV-202, CONV-204)') |
| `x_format_verdict__mutmut_41` | 971 | '', f"Otherwise detected: {changed['detected'] - changed['killed']}. " | 'XXXX', f"Otherwise detected: {changed['detected'] - changed['killed']}. " |
| `x_format_verdict__mutmut_47` | 974 | f"Failing: {len(failing)}.", ''] | f"Failing: {len(failing)}.", 'XXXX'] |
| `x_format_verdict__mutmut_49` | 976 | lines.extend(['Failing mutants on changed lines (these decide the outcome; ' | lines.extend(['XXFailing mutants on changed lines (these decide the outcome; XX' |
| `x_format_verdict__mutmut_52` | 977 | 'every one is in 'results.json'):', '']) | 'XXevery one is in 'results.json'):XX', '']) |
| `x_format_verdict__mutmut_53` | 977 | 'every one is in 'results.json'):', '']) | 'EVERY ONE IS IN 'RESULTS.JSON'):', '']) |
| `x_format_verdict__mutmut_54` | 977 | 'every one is in 'results.json'):', '']) | 'every one is in 'results.json'):', 'XXXX']) |
| `x_format_verdict__mutmut_69` | 983 | for title, values in (('Dispositioned as equivalent', equivalent), | for title, values in (('XXDispositioned as equivalentXX', equivalent), |
| `x_format_verdict__mutmut_70` | 983 | for title, values in (('Dispositioned as equivalent', equivalent), | for title, values in (('dispositioned as equivalent', equivalent), |
| `x_format_verdict__mutmut_71` | 983 | for title, values in (('Dispositioned as equivalent', equivalent), | for title, values in (('DISPOSITIONED AS EQUIVALENT', equivalent), |
| `x_format_verdict__mutmut_76` | 986 | lines.extend([f'{title}:', '']) | lines.extend([f'{title}:', 'XXXX']) |
| `x_format_verdict__mutmut_106` | 992 | else '') | else 'XXXX') |
| `x_format_verdict__mutmut_108` | 994 | lines.append('') | lines.append('XXXX') |
| `x_format_dispositions__mutmut_3` | 1039 | lines = ['## Mutation dispositions', '', | lines = ['XX## Mutation dispositionsXX', '', |
| `x_format_dispositions__mutmut_6` | 1039 | lines = ['## Mutation dispositions', '', | lines = ['## Mutation dispositions', 'XXXX', |
| `x_format_dispositions__mutmut_19` | 1043 | ''] | 'XXXX'] |
| `x_format_dispositions__mutmut_25` | 1045 | lines.extend([f"'{dispositions['file']}' is invalid; no entry applies:", '']) | lines.extend([f"'{dispositions['file']}' is invalid; no entry applies:", 'XXXX']) |
| `x_format_dispositions__mutmut_33` | 1047 | lines.append('') | lines.append('XXXX') |
| `x_format_dispositions__mutmut_40` | 1050 | lines.extend(['### Added entries: review each claim (CONV-203)', '']) | lines.extend(['XX### Added entries: review each claim (CONV-203)XX', '']) |
| `x_format_dispositions__mutmut_43` | 1050 | lines.extend(['### Added entries: review each claim (CONV-203)', '']) | lines.extend(['### Added entries: review each claim (CONV-203)', 'XXXX']) |
| `x_format_dispositions__mutmut_53` | 1052 | test = f" \| test {escape(record['test'])}" if record.get('test') else '' | test = f" \| test {escape(record['test'])}" if record.get('test') else 'XXXX' |
| `x_format_dispositions__mutmut_69` | 1056 | lines.append('') | lines.append('XXXX') |
| `x_format_dispositions__mutmut_76` | 1059 | lines.extend(['### Entries that fail this run', '']) | lines.extend(['XX### Entries that fail this runXX', '']) |
| `x_format_dispositions__mutmut_79` | 1059 | lines.extend(['### Entries that fail this run', '']) | lines.extend(['### Entries that fail this run', 'XXXX']) |
| `x_format_dispositions__mutmut_89` | 1062 | lines.append('') | lines.append('XXXX') |
| `x_format_dispositions__mutmut_105` | 1066 | lines.extend(['### Reported entries (not failing)', '']) | lines.extend(['XX### Reported entries (not failing)XX', '']) |
| `x_format_dispositions__mutmut_108` | 1066 | lines.extend(['### Reported entries (not failing)', '']) | lines.extend(['### Reported entries (not failing)', 'XXXX']) |
| `x_format_dispositions__mutmut_118` | 1069 | lines.append('') | lines.append('XXXX') |
| `x_format_dispositions__mutmut_123` | 1071 | lines.extend([f"'{ASSOCIATIONS_FILE}' is invalid; no association applies:", '']) | lines.extend([f"'{ASSOCIATIONS_FILE}' is invalid; no association applies:", 'XXXX']) |
| `x_format_dispositions__mutmut_131` | 1073 | lines.append('') | lines.append('XXXX') |
| `x_format_summary__mutmut_6` | 1079 | 'Blocking criterion: undetected mutants on the lines this pull request changed, ' | 'XXBlocking criterion: undetected mutants on the lines this pull request changed, XX' |
| `x_format_summary__mutmut_7` | 1079 | 'Blocking criterion: undetected mutants on the lines this pull request changed, ' | 'blocking criterion: undetected mutants on the lines this pull request changed, ' |
| `x_format_summary__mutmut_8` | 1079 | 'Blocking criterion: undetected mutants on the lines this pull request changed, ' | 'BLOCKING CRITERION: UNDETECTED MUTANTS ON THE LINES THIS PULL REQUEST CHANGED, ' |
| `x_format_summary__mutmut_10` | 1080 | 'unless a reviewed disposition accepts them (CONV-202). ' | 'unless a reviewed disposition accepts them (conv-202). ' |
| `x_format_summary__mutmut_11` | 1080 | 'unless a reviewed disposition accepts them (CONV-202). ' | 'UNLESS A REVIEWED DISPOSITION ACCEPTS THEM (CONV-202). ' |
| `x_format_summary__mutmut_9` | 1080 | 'unless a reviewed disposition accepts them (CONV-202). ' | 'XXunless a reviewed disposition accepts them (CONV-202). XX' |
| `x_read_selection__mutmut_31` | 1260 | raise ValueError('selection.json carries a malformed dispositions_base') | raise ValueError('XXselection.json carries a malformed dispositions_baseXX') |
| `x_check_dispositions__mutmut_128` | 1350 | return failing, '\n'.join(lines_out) | return failing, 'XX\nXX'.join(lines_out) |
| `x_remap_candidates__mutmut_20` | 1358 | tiers = (('same method, other instruction indexes', | tiers = (('XXsame method, other instruction indexesXX', |
| `x_remap_candidates__mutmut_26` | 1360 | ('renamed method of the same class', lambda candidate: candidate[0] == identity[0]), | ('XXrenamed method of the same classXX', lambda candidate: candidate[0] == identity[0]), |
| `x_remap_candidates__mutmut_32` | 1361 | ('same method name and descriptor in another class', | ('XXsame method name and descriptor in another classXX', |
| `x_remap_dispositions__mutmut_32` | 1384 | + (f'{len(matches)} candidate(s), {label}:' if label else 'no candidate')) | + (f'{len(matches)} candidate(s), {label}:' if label else 'XXno candidateXX')) |
| `x_remap_dispositions__mutmut_40` | 1387 | same = ', same instruction indexes' if indexes == identity_of(entry)[4] else '' | same = 'XX, same instruction indexesXX' if indexes == identity_of(entry)[4] else '' |
| `x_remap_dispositions__mutmut_45` | 1387 | same = ', same instruction indexes' if indexes == identity_of(entry)[4] else '' | same = ', same instruction indexes' if indexes == identity_of(entry)[4] else 'XXXX' |
| `x_counterfactual__mutmut_3` | 1427 | raise CounterfactualError('no test command given after '--'') | raise CounterfactualError('XXno test command given after '--'XX') |
| `x_counterfactual__mutmut_39` | 1445 | raise CounterfactualError('the patch does not apply to the working tree') | raise CounterfactualError('XXthe patch does not apply to the working treeXX') |
| `x_counterfactual__mutmut_67` | 1453 | + ', '.join(paths) + ' from version control before ' | + 'XX, XX'.join(paths) + ' from version control before ' |
| `x_counterfactual__mutmut_70` | 1454 | 'continuing') | 'XXcontinuingXX') |
| `x_counterfactual__mutmut_71` | 1454 | 'continuing') | 'CONTINUING') |
| `x_counterfactual__mutmut_75` | 1456 | raise CounterfactualError('the test passes with the mutant injected; it does not detect ' | raise CounterfactualError('XXthe test passes with the mutant injected; it does not detect XX' |
| `x_counterfactual__mutmut_77` | 1457 | 'this mutant') | 'XXthis mutantXX') |
| `x_counterfactual__mutmut_78` | 1457 | 'this mutant') | 'THIS MUTANT') |
| `x_counterfactual__mutmut_97` | 1461 | 'failure': '\n'.join(output[-FAILURE_EXCERPT_LINES:]) or '(no output)', | 'failure': '\n'.join(output[-FAILURE_EXCERPT_LINES:]) or 'XX(no output)XX', |
| `x_counterfactual__mutmut_98` | 1461 | 'failure': '\n'.join(output[-FAILURE_EXCERPT_LINES:]) or '(no output)', | 'failure': '\n'.join(output[-FAILURE_EXCERPT_LINES:]) or '(NO OUTPUT)', |
| `x_build_parser__mutmut_13` | 1469 | select = commands.add_parser('select', help='record changed sources and lines (CI)') | select = commands.add_parser('select', help=None) |
| `x_build_parser__mutmut_15` | 1469 | select = commands.add_parser('select', help='record changed sources and lines (CI)') | select = commands.add_parser('select', ) |
| `x_build_parser__mutmut_18` | 1469 | select = commands.add_parser('select', help='record changed sources and lines (CI)') | select = commands.add_parser('select', help='XXrecord changed sources and lines (CI)XX') |
| `x_build_parser__mutmut_19` | 1469 | select = commands.add_parser('select', help='record changed sources and lines (CI)') | select = commands.add_parser('select', help='record changed sources and lines (ci)') |
| `x_build_parser__mutmut_20` | 1469 | select = commands.add_parser('select', help='record changed sources and lines (CI)') | select = commands.add_parser('select', help='RECORD CHANGED SOURCES AND LINES (CI)') |
| `x_build_parser__mutmut_23` | 1470 | analysis = commands.add_parser('run', help='run bounded PIT and judge the result (CI)') | analysis = commands.add_parser('run', help=None) |
| `x_build_parser__mutmut_25` | 1470 | analysis = commands.add_parser('run', help='run bounded PIT and judge the result (CI)') | analysis = commands.add_parser('run', ) |
| `x_build_parser__mutmut_28` | 1470 | analysis = commands.add_parser('run', help='run bounded PIT and judge the result (CI)') | analysis = commands.add_parser('run', help='XXrun bounded PIT and judge the result (CI)XX') |
| `x_build_parser__mutmut_29` | 1470 | analysis = commands.add_parser('run', help='run bounded PIT and judge the result (CI)') | analysis = commands.add_parser('run', help='run bounded pit and judge the result (ci)') |
| `x_build_parser__mutmut_30` | 1470 | analysis = commands.add_parser('run', help='run bounded PIT and judge the result (CI)') | analysis = commands.add_parser('run', help='RUN BOUNDED PIT AND JUDGE THE RESULT (CI)') |
| `x_build_parser__mutmut_65` | 1478 | 'dispositions', help='local: check entries or propose identities after a refactor') | 'dispositions', help=None) |
| `x_build_parser__mutmut_67` | 1478 | 'dispositions', help='local: check entries or propose identities after a refactor') | 'dispositions', ) |
| `x_build_parser__mutmut_70` | 1478 | 'dispositions', help='local: check entries or propose identities after a refactor') | 'dispositions', help='XXlocal: check entries or propose identities after a refactorXX') |
| `x_build_parser__mutmut_71` | 1478 | 'dispositions', help='local: check entries or propose identities after a refactor') | 'dispositions', help='LOCAL: CHECK ENTRIES OR PROPOSE IDENTITIES AFTER A REFACTOR') |
| `x_build_parser__mutmut_75` | 1480 | help='a PIT mutations.xml; repeatable') | help=None) |
| `x_build_parser__mutmut_79` | 1480 | help='a PIT mutations.xml; repeatable') | ) |
| `x_build_parser__mutmut_85` | 1480 | help='a PIT mutations.xml; repeatable') | help='XXa PIT mutations.xml; repeatableXX') |
| `x_build_parser__mutmut_86` | 1480 | help='a PIT mutations.xml; repeatable') | help='a pit mutations.xml; repeatable') |
| `x_build_parser__mutmut_87` | 1480 | help='a PIT mutations.xml; repeatable') | help='A PIT MUTATIONS.XML; REPEATABLE') |
| `x_build_parser__mutmut_105` | 1483 | dispositions.add_argument('--base', help='commit the change is judged against (--check)') | dispositions.add_argument('--base', help=None) |
| `x_build_parser__mutmut_107` | 1483 | dispositions.add_argument('--base', help='commit the change is judged against (--check)') | dispositions.add_argument('--base', ) |
| `x_build_parser__mutmut_110` | 1483 | dispositions.add_argument('--base', help='commit the change is judged against (--check)') | dispositions.add_argument('--base', help='XXcommit the change is judged against (--check)XX') |
| `x_build_parser__mutmut_111` | 1483 | dispositions.add_argument('--base', help='commit the change is judged against (--check)') | dispositions.add_argument('--base', help='COMMIT THE CHANGE IS JUDGED AGAINST (--CHECK)') |
| `x_build_parser__mutmut_114` | 1485 | help='the reports come from a run with a per-class mutant limit') | help=None) |
| `x_build_parser__mutmut_117` | 1485 | help='the reports come from a run with a per-class mutant limit') | ) |
| `x_build_parser__mutmut_122` | 1485 | help='the reports come from a run with a per-class mutant limit') | help='XXthe reports come from a run with a per-class mutant limitXX') |
| `x_build_parser__mutmut_123` | 1485 | help='the reports come from a run with a per-class mutant limit') | help='THE REPORTS COME FROM A RUN WITH A PER-CLASS MUTANT LIMIT') |
| `x_build_parser__mutmut_126` | 1487 | 'counterfactual', help='local: produce killed-out-of-lane evidence for one mutant') | 'counterfactual', help=None) |
| `x_build_parser__mutmut_128` | 1487 | 'counterfactual', help='local: produce killed-out-of-lane evidence for one mutant') | 'counterfactual', ) |
| `x_build_parser__mutmut_131` | 1487 | 'counterfactual', help='local: produce killed-out-of-lane evidence for one mutant') | 'counterfactual', help='XXlocal: produce killed-out-of-lane evidence for one mutantXX') |
| `x_build_parser__mutmut_132` | 1487 | 'counterfactual', help='local: produce killed-out-of-lane evidence for one mutant') | 'counterfactual', help='LOCAL: PRODUCE KILLED-OUT-OF-LANE EVIDENCE FOR ONE MUTANT') |
| `x_build_parser__mutmut_135` | 1489 | help='unified diff injecting exactly the mutant') | help=None) |
| `x_build_parser__mutmut_138` | 1489 | help='unified diff injecting exactly the mutant') | ) |
| `x_build_parser__mutmut_142` | 1489 | help='unified diff injecting exactly the mutant') | help='XXunified diff injecting exactly the mutantXX') |
| `x_build_parser__mutmut_143` | 1489 | help='unified diff injecting exactly the mutant') | help='UNIFIED DIFF INJECTING EXACTLY THE MUTANT') |
| `x_build_parser__mutmut_146` | 1491 | help='after '--': the command that runs the test in its own process') | help=None) |
| `x_build_parser__mutmut_149` | 1491 | help='after '--': the command that runs the test in its own process') | ) |
| `x_build_parser__mutmut_152` | 1491 | help='after '--': the command that runs the test in its own process') | help='XXafter '--': the command that runs the test in its own processXX') |
| `x_build_parser__mutmut_153` | 1491 | help='after '--': the command that runs the test in its own process') | help='AFTER '--': THE COMMAND THAT RUNS THE TEST IN ITS OWN PROCESS') |
| `x_main__mutmut_11` | 1503 | parser.error('dispositions requires --check, --remap or both') | parser.error('XXdispositions requires --check, --remap or bothXX') |
| `x_main__mutmut_16` | 1505 | parser.error('dispositions --check requires --base') | parser.error('XXdispositions --check requires --baseXX') |

## equivalent (explicit default): 14

`subprocess` defaults to `shell=False`, and `None` is falsy, so the command runs identically. The keyword stays explicit because docs/VERIFICATION.md documents that every subprocess uses an argument list with `shell=False`, a security property reviewers check by reading the call.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_merge_base_file__mutmut_3` | 1192 | 'HEAD'], shell=False).decode().strip() | 'HEAD'], shell=None).decode().strip() |
| `x_merge_base_file__mutmut_5` | 1192 | 'HEAD'], shell=False).decode().strip() | 'HEAD'], ).decode().strip() |
| `x_merge_base_file__mutmut_19` | 1195 | shell=False) | shell=None) |
| `x_merge_base_file__mutmut_21` | 1195 | shell=False) | ) |
| `x_merge_base_file__mutmut_39` | 1199 | shell=False).decode('utf-8') | shell=None).decode('utf-8') |
| `x_merge_base_file__mutmut_41` | 1199 | shell=False).decode('utf-8') | ).decode('utf-8') |
| `x_current_commit__mutmut_2` | 1318 | shell=False).decode().strip() | shell=None).decode().strip() |
| `x_current_commit__mutmut_4` | 1318 | shell=False).decode().strip() | ).decode().strip() |
| `x_run_test_command__mutmut_10` | 1421 | shell=False) | ) |
| `x_run_test_command__mutmut_5` | 1421 | shell=False) | shell=None) |
| `x_counterfactual__mutmut_32` | 1444 | if subprocess.run(git + [str(patch_file)], shell=False).returncode: | if subprocess.run(git + [str(patch_file)], shell=None).returncode: |
| `x_counterfactual__mutmut_34` | 1444 | if subprocess.run(git + [str(patch_file)], shell=False).returncode: | if subprocess.run(git + [str(patch_file)], ).returncode: |
| `x_counterfactual__mutmut_48` | 1450 | shell=False).returncode == 0 | shell=None).returncode == 0 |
| `x_counterfactual__mutmut_50` | 1450 | shell=False).returncode == 0 | ).returncode == 0 |

## equivalent (unspecified layout): 3

CONV-202 specifies the content of `results.json`, not its indentation; every JSON reader parses the same document.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_write_results__mutmut_18` | 1185 | (output / 'results.json').write_text(json.dumps(document, indent=2) + '\n') | (output / 'results.json').write_text(json.dumps(document, indent=None) + '\n') |
| `x_write_results__mutmut_20` | 1185 | (output / 'results.json').write_text(json.dumps(document, indent=2) + '\n') | (output / 'results.json').write_text(json.dumps(document, ) + '\n') |
| `x_write_results__mutmut_21` | 1185 | (output / 'results.json').write_text(json.dumps(document, indent=2) + '\n') | (output / 'results.json').write_text(json.dumps(document, indent=3) + '\n') |

## equivalent (sentinel): 2

An unparsable PIT index must never equal a validated entry index, which is a non-negative integer (CONV-201); `-1`, `-2` and `None` all satisfy that. `+1` is killed by a test.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_mutant_identity__mutmut_7` | 793 | indexes.append(-1)  # never equal to a validated entry index | indexes.append(None)  # never equal to a validated entry index |
| `x_mutant_identity__mutmut_9` | 793 | indexes.append(-1)  # never equal to a validated entry index | indexes.append(-2)  # never equal to a validated entry index |

## equivalent (initial state): 1

Only a previous line starting with `--- ` marks a header; any other initial value, including `XXXX`, gives the same first-line decision.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_patch_paths__mutmut_2` | 551 | paths, previous = [], '' | paths, previous = [], 'XXXX' |

## equivalent (falsy initial value): 1

`touched` is only read as `touched and ...`; `[]` and `None` are both falsy when the patch was rejected before paths were read.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_evidence_problems__mutmut_13` | 573 | touched = [] | touched = None |

## equivalent (grammar): 1

CONV-201 validates `test` against `<class>#<method>` with exactly one `#`, so `partition` and `rpartition` split it identically.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_static_staleness__mutmut_30` | 827 | test_class = entry['test'].partition('#')[0].partition('$')[0] | test_class = entry['test'].rpartition('#')[0].partition('$')[0] |

## equivalent (fixed path): 1

`path` is the constant `.github/mutation-dispositions.toml`, never option-like, so `git ls-tree` lists it with or without the `--` separator (an extra path argument matches nothing).

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_merge_base_file__mutmut_34` | 1194 | '--name-only', commit_id(merge_base), '--', path], | '--name-only', commit_id(merge_base), 'XX--XX', path], |

## equivalent (codec alias): 1

Python codec names are case-insensitive: `UTF-8` and `utf-8` name the same codec.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_merge_base_file__mutmut_51` | 1199 | shell=False).decode('utf-8') | shell=False).decode('UTF-8') |

## equivalent (unused result): 1

The check helper reports file errors from its own list and never reads the verdict's `errors` or `clean` fields, which are the only uses of this argument.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_check_dispositions__mutmut_51` | 1336 | verdict, accepted = judge_dispositions([] if errors else entries, errors, | verdict, accepted = judge_dispositions([] if errors else entries, None, |

## equivalent (unreachable member): 1

The set only matters for characters below U+0020; adding `X` to it changes no control character decision.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_toml_string__mutmut_15` | 1401 | and not any((ord(character) < 32 and character not in '\n\t') or ord(character) == 127 | and not any((ord(character) < 32 and character not in 'XX\n\tXX') or ord(character) == 127 |

## equivalent (falsy default): 1

`json.dumps` treats `None` like `False`: non-ASCII text stays literal, as TOML allows.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_toml_string__mutmut_28` | 1404 | return json.dumps(value, ensure_ascii=False).replace('\x7f', '\\u007f') | return json.dumps(value, ensure_ascii=None).replace('\x7f', '\\u007f') |

## equivalent (escape case): 1

TOML `\uXXXX` escapes accept upper- and lower-case hexadecimal digits; tomllib reads both to U+007F.

| Mutant | Line | Original | Mutated |
|---|---|---|---|
| `x_toml_string__mutmut_34` | 1404 | return json.dumps(value, ensure_ascii=False).replace('\x7f', '\\u007f') | return json.dumps(value, ensure_ascii=False).replace('\x7f', '\\u007F') |
