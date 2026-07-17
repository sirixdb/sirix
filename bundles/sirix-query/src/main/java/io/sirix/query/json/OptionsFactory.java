package io.sirix.query.json;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.ValidTimeConfig;
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
    final Sequence buildPathStatisticsSequence = providedOptions.get(new QNm("buildPathStatistics"));
    final Sequence storageTypeSequence = providedOptions.get(new QNm("storageType"));
    final Sequence useDeweyIDsSequence = providedOptions.get(new QNm("useDeweyIDs"));
    final Sequence hashTypeSequence = providedOptions.get(new QNm("hashType"));
    final Sequence versioningTypeSequence = providedOptions.get(new QNm("versionType"));
    final Sequence numberOfNodesBeforeAutoCommitSequence =
        providedOptions.get(new QNm("numberOfNodesBeforeAutoCommit"));
    final Sequence validFromPathSequence = providedOptions.get(new QNm("validFromPath"));
    final Sequence validToPathSequence = providedOptions.get(new QNm("validToPath"));
    final Sequence useConventionalValidTimeSequence = providedOptions.get(new QNm("useConventionalValidTime"));
    final Sequence autoCreateValidTimeIndexSequence = providedOptions.get(new QNm("autoCreateValidTimeIndex"));

    final String commitMessage = commitMessageSequence != null
        ? ((Str) commitMessageSequence).stringValue()
        : null;
    final Instant commitTimestamp = dateTimeSequence != null
        ? DATE_TIME_TO_INSTANT.convert(new DateTime(dateTimeSequence.toString()))
        : null;
    final boolean useTextCompression = useTextCompressionSequence != null && useTextCompressionSequence.booleanValue();
    final boolean buildPathSummary = buildPathSummarySequence == null
        ? defaultOptions.buildPathSummary()
        : buildPathSummarySequence.booleanValue();
    final boolean buildPathStatistics = buildPathStatisticsSequence == null
        ? defaultOptions.buildPathStatistics()
        : buildPathStatisticsSequence.booleanValue();
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
    final ValidTimeConfig validTimeConfig =
        resolveValidTimeConfig(validFromPathSequence, validToPathSequence, useConventionalValidTimeSequence,
            defaultOptions);
    final boolean autoCreateValidTimeIndex =
        toBoolean(autoCreateValidTimeIndexSequence, "autoCreateValidTimeIndex", defaultOptions.autoCreateValidTimeIndex());
    return new Options(commitMessage, commitTimestamp, useTextCompression, buildPathSummary,
        buildPathStatistics, storageType, useDeweyIDs,
        hashType, versioningType, numberOfNodesBeforeAutoCommit, defaultOptions.storeNodeHistory(),
        validTimeConfig, autoCreateValidTimeIndex);
  }

  /**
   * Resolve the valid-time (bitemporal) configuration from the provided options. Mirrors the REST
   * API's resource-creation parameters: {@code useConventionalValidTime} wins over explicit paths;
   * explicit paths must be given as a pair.
   */
  private static ValidTimeConfig resolveValidTimeConfig(final Sequence validFromPathSequence,
      final Sequence validToPathSequence, final Sequence useConventionalValidTimeSequence,
      final Options defaultOptions) {
    final boolean useConventionalValidTime =
        toBoolean(useConventionalValidTimeSequence, "useConventionalValidTime", false);
    if (useConventionalValidTime) {
      return ValidTimeConfig.withConventionalPaths();
    }
    final String validFromPath = toStringValue(validFromPathSequence, "validFromPath");
    final String validToPath = toStringValue(validToPathSequence, "validToPath");
    if (validFromPath != null && validToPath != null) {
      if (validFromPath.isEmpty() || validToPath.isEmpty()) {
        throw new QueryException(new QNm("Options validFromPath and validToPath must not be empty."));
      }
      return ValidTimeConfig.withPaths(validFromPath, validToPath);
    }
    if (validFromPath != null || validToPath != null) {
      throw new QueryException(new QNm("Options validFromPath and validToPath must be specified together."));
    }
    return defaultOptions.validTimeConfig();
  }

  /**
   * Interpret a boolean option value. Unlike the raw XQuery effective boolean value (under which
   * any non-empty string — including {@code "false"} — is {@code true}), a string value is parsed
   * textually so {@code {"option": "false"}} behaves as users expect.
   */
  private static boolean toBoolean(final Sequence sequence, final String optionName, final boolean defaultValue) {
    if (sequence == null) {
      return defaultValue;
    }
    if (sequence instanceof Str str) {
      final String value = str.stringValue();
      if ("true".equalsIgnoreCase(value)) {
        return true;
      }
      if ("false".equalsIgnoreCase(value)) {
        return false;
      }
      throw new QueryException(new QNm("Option " + optionName + " must be a boolean (got: '" + value + "')."));
    }
    if (sequence instanceof Bool bool) {
      return bool.booleanValue();
    }
    return sequence.booleanValue();
  }

  /**
   * Extract a string option value with a clear error message instead of a raw
   * {@link ClassCastException} for non-string values.
   */
  private static String toStringValue(final Sequence sequence, final String optionName) {
    if (sequence == null) {
      return null;
    }
    if (sequence instanceof Str str) {
      return str.stringValue();
    }
    throw new QueryException(new QNm("Option " + optionName + " must be a string."));
  }
}
