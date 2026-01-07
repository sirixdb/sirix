package io.sirix.node;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

/**
 * Micro-benchmark to verify performance of the optimized DeweyID encoder.
 * 
 * <p>This is not a proper JMH benchmark but provides a quick sanity check
 * that the optimized encoder is performing as expected.
 */
class DeweyIDEncoderBenchmark {
    
    private static final int WARMUP_ITERATIONS = 10_000;
    private static final int BENCHMARK_ITERATIONS = 100_000;
    
    @Test
    @DisplayName("Benchmark DeweyIDEncoder.encode() throughput")
    void benchmarkEncoder() {
        // Generate test data
        Random random = new Random(42);
        int[][] testDivisions = new int[1000][];
        for (int i = 0; i < testDivisions.length; i++) {
            int numDivisions = random.nextInt(8) + 1;
            testDivisions[i] = new int[numDivisions + 1];
            testDivisions[i][0] = 1;
            for (int j = 1; j <= numDivisions; j++) {
                // Mix of different tiers
                int tier = random.nextInt(100);
                if (tier < 70) {
                    testDivisions[i][j] = random.nextInt(127);        // Tier 0 (70%)
                } else if (tier < 95) {
                    testDivisions[i][j] = 127 + random.nextInt(16384); // Tier 1 (25%)
                } else {
                    testDivisions[i][j] = 16511 + random.nextInt(100000); // Tier 2 (5%)
                }
            }
        }
        
        // Warmup
        for (int w = 0; w < WARMUP_ITERATIONS; w++) {
            for (int[] divisions : testDivisions) {
                DeweyIDEncoder.encode(divisions);
            }
        }
        
        // Benchmark
        long totalBytes = 0;
        long startTime = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            for (int[] divisions : testDivisions) {
                byte[] encoded = DeweyIDEncoder.encode(divisions);
                totalBytes += encoded.length;
            }
        }
        long endTime = System.nanoTime();
        
        long totalEncodings = (long) BENCHMARK_ITERATIONS * testDivisions.length;
        double durationMs = (endTime - startTime) / 1_000_000.0;
        double encodingsPerSecond = totalEncodings / (durationMs / 1000.0);
        double bytesPerSecond = totalBytes / (durationMs / 1000.0);
        
        System.out.println("\n--- DeweyIDEncoder Benchmark Results ---");
        System.out.printf("Total encodings: %,d%n", totalEncodings);
        System.out.printf("Total bytes: %,d%n", totalBytes);
        System.out.printf("Duration: %.2f ms%n", durationMs);
        System.out.printf("Throughput: %,.0f encodings/second%n", encodingsPerSecond);
        System.out.printf("Throughput: %,.0f bytes/second (%.2f MB/s)%n", 
            bytesPerSecond, bytesPerSecond / 1_000_000.0);
    }
    
    @Test
    @DisplayName("Benchmark SirixDeweyID.toBytes() end-to-end")
    void benchmarkToBytesEndToEnd() {
        // Generate test DeweyIDs
        Random random = new Random(42);
        SirixDeweyID[] testIds = new SirixDeweyID[1000];
        for (int i = 0; i < testIds.length; i++) {
            StringBuilder sb = new StringBuilder("1");
            int numDivisions = random.nextInt(8) + 1;
            for (int j = 0; j < numDivisions; j++) {
                sb.append(".");
                int tier = random.nextInt(100);
                if (tier < 70) {
                    sb.append(random.nextInt(127));        // Tier 0
                } else if (tier < 95) {
                    sb.append(127 + random.nextInt(16384)); // Tier 1
                } else {
                    sb.append(16511 + random.nextInt(100000)); // Tier 2
                }
            }
            testIds[i] = new SirixDeweyID(sb.toString());
        }
        
        // Warmup
        for (int w = 0; w < WARMUP_ITERATIONS; w++) {
            for (SirixDeweyID id : testIds) {
                // Clear cached bytes to force re-encoding
                SirixDeweyID fresh = new SirixDeweyID(id.getDivisionValues());
                fresh.toBytes();
            }
        }
        
        // Benchmark
        long totalBytes = 0;
        long startTime = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            for (SirixDeweyID id : testIds) {
                SirixDeweyID fresh = new SirixDeweyID(id.getDivisionValues());
                byte[] encoded = fresh.toBytes();
                totalBytes += encoded.length;
            }
        }
        long endTime = System.nanoTime();
        
        long totalEncodings = (long) BENCHMARK_ITERATIONS * testIds.length;
        double durationMs = (endTime - startTime) / 1_000_000.0;
        double encodingsPerSecond = totalEncodings / (durationMs / 1000.0);
        
        System.out.println("\n--- SirixDeweyID.toBytes() Benchmark Results ---");
        System.out.printf("Total encodings: %,d%n", totalEncodings);
        System.out.printf("Total bytes: %,d%n", totalBytes);
        System.out.printf("Duration: %.2f ms%n", durationMs);
        System.out.printf("Throughput: %,.0f DeweyID.toBytes()/second%n", encodingsPerSecond);
    }
    
    @Test
    @DisplayName("Verify tier distribution of test data")
    void verifyTierDistribution() {
        Random random = new Random(42);
        int[] tierCounts = new int[5];
        int totalDivisions = 0;
        
        for (int i = 0; i < 10000; i++) {
            int numDivisions = random.nextInt(8) + 1;
            for (int j = 0; j < numDivisions; j++) {
                int tier = random.nextInt(100);
                int value;
                if (tier < 70) {
                    value = random.nextInt(127);        // Tier 0
                } else if (tier < 95) {
                    value = 127 + random.nextInt(16384); // Tier 1
                } else {
                    value = 16511 + random.nextInt(100000); // Tier 2
                }
                tierCounts[DeweyIDEncoder.getTier(value)]++;
                totalDivisions++;
            }
        }
        
        System.out.println("\n--- Tier Distribution ---");
        for (int t = 0; t < 5; t++) {
            double pct = 100.0 * tierCounts[t] / totalDivisions;
            System.out.printf("Tier %d: %,d (%.1f%%)%n", t, tierCounts[t], pct);
        }
    }
}

