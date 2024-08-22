package io.sirix.query.json;

import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.trx.node.HashType;
import io.sirix.io.StorageType;

import java.time.Instant;

import io.brackit.query.jdm.json.Object;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.settings.VersioningType;

class OptionsFactory {

	private static final DateTimeToInstant DATE_TIME_TO_INSTANT = new DateTimeToInstant();

	public static Options createOptions(Object providedOptions, Options defaultOptions) {
		final Sequence commitMessageSequence = providedOptions.get(new QNm("commitMessage"));
		final Sequence dateTimeSequence = providedOptions.get(new QNm("commitTimestamp"));
		final Sequence useTextCompressionSequence = providedOptions.get(new QNm("useTextCompression"));
		final Sequence buildPathSummarySequence = providedOptions.get(new QNm("buildPathSummary"));
		final Sequence storageTypeSequence = providedOptions.get(new QNm("storageType"));
		final Sequence useDeweyIDsSequence = providedOptions.get(new QNm("useDeweyIDs"));
		final Sequence hashTypeSequence = providedOptions.get(new QNm("hashType"));
		final Sequence versioningTypeSequence = providedOptions.get(new QNm("versionType"));
		final Sequence numberOfNodesBeforeAutoCommitSequence = providedOptions
				.get(new QNm("numberOfNodesBeforeAutoCommit"));

		final String commitMessage = commitMessageSequence != null ? ((Str) commitMessageSequence).stringValue() : null;
		final Instant commitTimestamp = dateTimeSequence != null
				? DATE_TIME_TO_INSTANT.convert(new DateTime(dateTimeSequence.toString()))
				: null;
		final boolean useTextCompression = useTextCompressionSequence != null
				&& useTextCompressionSequence.booleanValue();
		final boolean buildPathSummary = buildPathSummarySequence == null
				? defaultOptions.buildPathSummary()
				: buildPathSummarySequence.booleanValue();
		final StorageType storageType = storageTypeSequence == null
				? defaultOptions.storageType()
				: StorageType.valueOf(storageTypeSequence.toString());
		final boolean useDeweyIDs = useDeweyIDsSequence == null
				? defaultOptions.useDeweyIDs()
				: useDeweyIDsSequence.booleanValue();
		final HashType hashType = hashTypeSequence == null
				? defaultOptions.hashType()
				: HashType.fromString(hashTypeSequence.toString());
		final VersioningType versioningType = versioningTypeSequence == null
				? defaultOptions.versioningType()
				: VersioningType.fromString(versioningTypeSequence.toString());
		final int numberOfNodesBeforeAutoCommit = numberOfNodesBeforeAutoCommitSequence == null
				? defaultOptions.numberOfNodesBeforeAutoCommit()
				: ((Int64) numberOfNodesBeforeAutoCommitSequence).intValue();
		return new Options(commitMessage, commitTimestamp, useTextCompression, buildPathSummary, storageType,
				useDeweyIDs, hashType, versioningType, numberOfNodesBeforeAutoCommit);
	}
}
