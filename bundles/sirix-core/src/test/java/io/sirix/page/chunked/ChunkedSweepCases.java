/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.page.chunked.ChunkedPageGenerator.Body;
import io.sirix.page.chunked.ChunkedPageGenerator.Hash;
import io.sirix.page.chunked.ChunkedPageGenerator.Names;
import io.sirix.page.chunked.ChunkedPageGenerator.ParentKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.PathKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.Recipe;
import io.sirix.page.chunked.ChunkedPageGenerator.Shape;
import io.sirix.page.chunked.ChunkedPageGenerator.Sizes;
import io.sirix.page.chunked.ChunkedPageGenerator.Values;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The pages the chunked-body sweeps run over, so the format sweep and the lazy-expansion sweep cover
 * exactly the same ground.
 *
 * <p>
 * The structural levers, page shapes, entry counts, record sizes and chunk targets are a full
 * cross-product within each group; the group split follows the plan's core-plus-overlay pattern,
 * because a single product over every axis would multiply out to hundreds of thousands of pages for
 * coverage the axes do not interact to produce.
 */
final class ChunkedSweepCases {

  private ChunkedSweepCases() {}

  /** One page to build, and the chunk target to frame it at. */
  record Case(Recipe recipe, int targetChunkBytes) {
    boolean dewey() {
      return recipe.deweyIds();
    }

    @Override
    public String toString() {
      return recipe + " @C=" + targetChunkBytes;
    }
  }

  /** Every case, grouped by what the group is there to provoke. Insertion-ordered. */
  static Map<String, List<Case>> byGroup() {
    final Map<String, List<Case>> groups = new LinkedHashMap<>();
    groups.put("core lever cross-product", coreCases());
    groups.put("column-activation overlay", columnCases());
    groups.put("page-shape overlay", shapeCases());
    groups.put("record-size overlay", sizeCases());
    groups.put("DeweyID overlay", deweyCases());
    groups.put("degenerate-body arm", degenerateCases());
    return groups;
  }

  /** Every case, flattened. */
  static List<Case> all() {
    final List<Case> cases = new ArrayList<>();
    for (final List<Case> group : byGroup().values()) {
      cases.addAll(group);
    }
    return cases;
  }

  /**
   * The full product of the five content levers, at a page shape and entry count that straddle chunk
   * boundaries: 65 records is more than one chunk at the small target and exactly one at the default.
   */
  private static List<Case> coreCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Hash hash : Hash.values()) {
      for (final ParentKeys parentKeys : ParentKeys.values()) {
        for (final PathKeys pathKeys : PathKeys.values()) {
          for (final Values values : Values.values()) {
            for (final Names names : Names.values()) {
              for (final int target : new int[] {64, 4096}) {
                cases.add(new Case(new Recipe(Body.TEMPLATED, hash, parentKeys, pathKeys, values, names, Shape.DENSE,
                    Sizes.SMALL, 65, false), target));
              }
            }
          }
        }
      }
    }
    return cases;
  }

  /**
   * Bigger pages, where the dictionary columns start to pay for themselves: a column only activates
   * when its encoding comes out smaller than the varints it displaces, which a 65-record page rarely
   * manages and a 512-record one usually does.
   */
  private static List<Case> columnCases() {
    final List<Case> cases = new ArrayList<>();
    for (final int entries : new int[] {256, 512}) {
      for (final PathKeys pathKeys : PathKeys.values()) {
        for (final Hash hash : Hash.values()) {
          for (final Values values : new Values[] {Values.MIXED, Values.STRUCTURAL, Values.MIXED_STRUCTURAL}) {
            for (final Names names : new Names[] {Names.WIDE, Names.MANY}) {
              cases.add(new Case(new Recipe(Body.TEMPLATED, hash, ParentKeys.SEQUENTIAL, pathKeys, values, names,
                  Shape.DENSE, Sizes.SMALL, entries, false), 4096));
            }
          }
        }
      }
    }
    return cases;
  }

  /**
   * Bitmap shapes and entry counts, including the counts that sit either side of a chunk boundary.
   */
  private static List<Case> shapeCases() {
    final List<Case> cases = new ArrayList<>();
    final Recipe[] profiles = {
        new Recipe(Body.TEMPLATED, Hash.ALL_ZERO, ParentKeys.SEQUENTIAL, PathKeys.SINGLE, Values.MIXED, Names.WIDE,
            Shape.DENSE, Sizes.SMALL, 0, false),
        new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.ALL_NULL, PathKeys.DISTINCT, Values.NUMBERS, Names.FEW,
            Shape.DENSE, Sizes.MIXED, 0, false)};
    for (final Shape shape : Shape.values()) {
      for (final int entries : new int[] {0, 1, 2, 63, 64, 65, 511, 512}) {
        if (entries > ChunkedPageGenerator.capacity(shape)) {
          continue;
        }
        for (final int target : new int[] {64, 4096}) {
          for (final Recipe profile : profiles) {
            cases.add(new Case(withShape(profile, shape, entries), target));
          }
        }
      }
    }
    return cases;
  }

  /**
   * Record sizes, including one record larger than the chunk target and one near the record ceiling.
   */
  private static List<Case> sizeCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Sizes sizes : Sizes.values()) {
      for (final int entries : new int[] {8, 64}) {
        for (final int target : new int[] {64, 4096, 1 << 20}) {
          cases.add(new Case(new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.SEQUENTIAL, PathKeys.FEW,
              Values.STRINGS, Names.WIDE, Shape.DENSE, sizes, entries, false), target));
        }
      }
    }
    return cases;
  }

  /** DeweyID trailers live inside the heap, so they ride the chunks rather than any slot's bytes. */
  private static List<Case> deweyCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Values values : Values.values()) {
      for (final int target : new int[] {64, 4096}) {
        cases.add(new Case(new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.MIXED_NULL, PathKeys.FEW, values,
            Names.WIDE, Shape.DENSE, Sizes.MIXED, 33, true), target));
      }
    }
    return cases;
  }

  /** The degenerate body: META is the compact dir alone and the chunks are verbatim records. */
  private static List<Case> degenerateCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Shape shape : Shape.values()) {
      for (final int entries : new int[] {0, 1, 2, 64, 512}) {
        if (entries > ChunkedPageGenerator.capacity(shape)) {
          continue;
        }
        for (final Sizes sizes : Sizes.values()) {
          for (final int target : new int[] {64, 4096}) {
            cases.add(new Case(new Recipe(Body.DEGENERATE, Hash.NONE_ZERO, ParentKeys.SEQUENTIAL, PathKeys.SINGLE,
                Values.NUMBERS, Names.ONE, shape, sizes, entries, false), target));
          }
        }
      }
    }
    return cases;
  }

  private static Recipe withShape(final Recipe profile, final Shape shape, final int entries) {
    return new Recipe(profile.body(), profile.hash(), profile.parentKeys(), profile.pathKeys(), profile.values(),
        profile.names(), shape, profile.sizes(), entries, profile.deweyIds());
  }
}
