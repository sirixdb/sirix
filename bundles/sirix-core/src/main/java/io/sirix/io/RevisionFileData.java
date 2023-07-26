package io.sirix.io;

import java.time.Instant;

public record RevisionFileData(long offset, Instant timestamp) {
}
