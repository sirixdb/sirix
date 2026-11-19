# Preregistered 24-block CodeFactor follow-up

Authorized by instruction028 at2026-11-19T01:47:14Z, before this experiment.
The original4-block trial remains HOLD and is not pooled with this trial.
Baseline9732747e47fc549960256af9d4516644badfda1e; candidate is the unchanged helper-extraction patch with production source hashes in ../nm-completed-timing/local-source-comparison.json. No production tuning between trials or within this trial.
Exactly24ABBA blocks/96freshJVMs, order before/after/after/before; all5cases insertAsc4096,insertDesc4096,insertDesc50000,commitDesc4096,commitDesc50000. Each case30srealwarmup+50JMHwarmupbatches+36retained measurements, defaultasyncC2. All samples and all trials retained.
CPU0; exclusive inherited-lock window through /home/johannes/IdeaProjects/firstmate/data/tools/heavy.sh; wait for other lanes timing work, then3consecutive30s quiet observations with1minload<4 and5minload<6. Disk>=60GiB before admission and each JVM. No JVM outsideheavy.sh.
Symmetric ordered classpaths, equal-length before/after_ production paths, frozen classes/dependencies, no concurrent builds. Candidate/baseline and harness hashes recorded before samples.
Decision unchanged: paired mean block-log-ratio point<=+1%, lower95%logratio<=0, timedGC=0;24paired block contrasts withdf23 Student t critical2.0686576104190406. Report95%intervals,allprocess allocation separately from exactsubsystembudgets. No adaptive reruns,discarding, or tuning to getgreen.
IfHOLDpersists, reportprecision and method/profile evidence; instruction028 allows inspecting commit-path helper extraction and keeping the responsible method inline when a narrow interval supports cost, then reporting. No change before the complete24-block analysis.
Timing may run during daytime; no commit,push,PRpublication before17:30Berlin2026-11-19(16:30UTC). Stop on resource/preconditionfailure and preservepartialdata.

Candidate production SHA-256 (fixed before this experiment):
bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java 611437a7615b1742ca5a4240fc5cd3335ecfa2a5628bdd3775c7e6dc281a6b85
bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTIncrementalInsert.java 921868728c18e59ec75ff383acc9bfac4754d3d38e879bbbed3d0d7f273b1193
bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTCommitNormalizer.java 20f29f512ea1b03a051f809a45fc6bf3b11eac14fea1058094a9da52ef346b56

Fresh compiled-shape analysis includes successful kernel logs from the corrected frozen fixture; the old report's skip for its historical failed kernel launch does not apply to this replay. This does not alter production or timing code.

## Instruction029 scheduling correction
The first24-block attempt was stopped on explicit Firstmate instruction with58of96JVMs complete. All partial data and original controls are retained underinterrupted-029; no partial scores were analyzed or used for tuning. The retry remains the same unchangedcandidate/baseline and fixed24-block protocol, and starts onlyafterthewriter gate reportsallblocks done. Timing usesdata/tools/timing-window.sh DIRECTLY withFM_TIMING_CPUS=0, nevernestedinsideheavy.sh. This explicit instruction supersedes the earlier timing-wrapper requirement for this trial. All ordinaryGradle/JVMverification stillusesheavy.sh. Quietcriteria,diskfloor,parameters,acceptance,andnocommitbefore17:30Berlin remainunchanged.

## Instructions030–031 queue clearance
Firstmate confirmedwriter gates5/6passed and instructedqueue directlybehindcodecSH1, expectedtoholdtimingwindowsuntil19:40Berlin2026-11-19. Queue+execution watchdogextendedfrom7hto24h solelytoallowthat9.5hqueue; actualmeasurement executioncapremains6h andallmeasurement parameters/acceptanceunchanged. Writercompletionconditioniscleared; noexperimentrestartearlierthanthenamedpredecessorwindow. No publicationbefore17:30Berlin; ordinaryweekdaypublic-actioncutoffstillappliesaftertherun.
