package io.sirix.query.json;

import io.brackit.query.QueryException;
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

  private static final QNm COMMIT_MESSAGE = new QNm("commitMessage");
  private static final QNm COMMIT_TIMESTAMP = new QNm("commitTimestamp");
  private static final QNm USE_TEXT_COMPRESSION = new QNm("useTextCompression");
  private static final QNm BUILD_PATH_SUMMARY = new QNm("buildPathSummary");
  private static final QNm BUILD_PATH_STATISTICS = new QNm("buildPathStatistics");
  private static final QNm STORAGE_TYPE = new QNm("storageType");
  private static final QNm USE_DEWEY_IDS = new QNm("useDeweyIDs");
  private static final QNm HASH_TYPE = new QNm("hashType");
  private static final QNm VERSION_TYPE = new QNm("versionType");
  private static final QNm NUMBER_OF_NODES_BEFORE_AUTO_COMMIT = new QNm("numberOfNodesBeforeAutoCommit");
  private static final QNm VALID_FROM_PATH = new QNm("validFromPath");
  private static final QNm VALID_TO_PATH = new QNm("validToPath");
  private static final QNm USE_CONVENTIONAL_VALID_TIME = new QNm("useConventionalValidTime");
  private static final QNm AUTO_CREATE_VALID_TIME_INDEX = new QNm("autoCreateValidTimeIndex");

  public static Options createOptions(Object providedOptions, Options defaultOptions) {
    final Sequence commitMessageSequence = providedOptions.get(COMMIT_MESSAGE);
    final Sequence dateTimeSequence = providedOptions.get(COMMIT_TIMESTAMP);
    final Sequence storageTypeSequence = providedOptions.get(STORAGE_TYPE);
    final Sequence hashTypeSequence = providedOptions.get(HASH_TYPE);
    final Sequence versioningTypeSequence = providedOptions.get(VERSION_TYPE);
    final Sequence numberOfNodesBeforeAutoCommitSequence = providedOptions.get(NUMBER_OF_NODES_BEFORE_AUTO_COMMIT);

    final String commitMessage = commitMessageSequence != null
        ? ((Str) commitMessageSequence).stringValue()
        : null;
    final Instant commitTimestamp = dateTimeSequence != null
        ? DATE_TIME_TO_INSTANT.convert(new DateTime(dateTimeSequence.toString()))
        : null;
    final boolean useTextCompression =
        toBoolean(providedOptions.get(USE_TEXT_COMPRESSION), "useTextCompression", false);
    final boolean buildPathSummary =
        toBoolean(providedOptions.get(BUILD_PATH_SUMMARY), "buildPathSummary", defaultOptions.buildPathSummary());
    final boolean buildPathStatistics = toBoolean(providedOptions.get(BUILD_PATH_STATISTICS), "buildPathStatistics",
        defaultOptions.buildPathStatistics());
    final StorageType storageType = storageTypeSequence == null
        ? defaultOptions.storageType()
        : StorageType.valueOf(storageTypeSequence.toString());
    final boolean useDeweyIDs =
        toBoolean(providedOptions.get(USE_DEWEY_IDS), "useDeweyIDs", defaultOptions.useDeweyIDs());
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
        resolveValidTimeConfig(providedOptions.get(VALID_FROM_PATH), providedOptions.get(VALID_TO_PATH),
            providedOptions.get(USE_CONVENTIONAL_VALID_TIME), defaultOptions);
    final boolean autoCreateValidTimeIndex = toBoolean(providedOptions.get(AUTO_CREATE_VALID_TIME_INDEX),
        "autoCreateValidTimeIndex", defaultOptions.autoCreateValidTimeIndex());
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
