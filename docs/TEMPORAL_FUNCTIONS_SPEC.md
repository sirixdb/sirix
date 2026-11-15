# Temporal navigation functions: specification

Status: normative for `jn:first`, `jn:last`, `jn:previous`, `jn:next`, `jn:past`,
`jn:future` and `jn:all-times` (issue [#1429](https://github.com/sirixdb/sirix/issues/1429)).
Tests: `TemporalNavigationTest` (`bundles/sirix-query/src/test/java/io/sirix/query/function/jn/temporal/`).

## Domain

Every value in a Sirix JSON resource is a node with a node key that is stable across revisions.
The temporal functions therefore accept any **stored item**: an item read from a Sirix
resource that carries a node key. Stored items are

| Kind | Example source |
| --- | --- |
| object, array | `jn:doc('db','res')[0]`, `jn:doc('db','res').tags` |
| number (integer, decimal, double, float) | `jn:doc('db','res')[0].price`, `jn:doc('db','res')[2]` |
| string | `jn:doc('db','res')[0].name` |
| boolean | `jn:doc('db','res')[0].inStock` |
| `null` | `jn:doc('db','res')[0].discount` |
| object field name | an item of `bit:fields($object)` |

Values at array positions and values of object fields are both covered; under record fusion an
object field and its primitive value share one node.

Node identity follows the update operation, not the path: an in-place value update and
`replace json value` of an object field keep the node, so its history spans the update, while
`replace json value` of an array member inserts a new node whose history starts in that revision.

## Behaviour

Let `$i` be a stored item read in revision `r` of a resource whose most recent revision is `m`,
and let `k` be its node key. "The node in revision `s`" means the node with key `k` as read in
revision `s`.

| Function | Result |
| --- | --- |
| `jn:first($i)` | the node in revision 1 |
| `jn:last($i)` | the node in revision `m` |
| `jn:previous($i)` | the node in revision `r - 1`; empty when `r = 1` |
| `jn:next($i)` | the node in revision `r + 1`; empty when `r = m` |
| `jn:past($i, $includeSelf := false())` | the nodes in revisions `r - 1` (or `r`) down to 1, newest first, stopping at the first revision without the node |
| `jn:future($i, $includeSelf := false())` | the nodes in revisions `r + 1` (or `r`) up to `m`, oldest first, stopping at the first revision without the node |
| `jn:all-times($i)` | the nodes in revisions 1 to `m`, oldest first, from the first revision with the node up to the first later revision without it |

Invariants:

1. **Same node, value as of that revision.** Each result has node key `k` and the revision the
   function names (`sdb:revision` reports it). Its value and type are those the node holds in that
   revision: a number updated from `999` to `899` yields `999` from `jn:first` and `899` from
   `jn:last`. A result is the same kind of item a read of that revision would produce
   (`sdb:select-item(jn:doc('db','res', s), k)`); a field name stays a field name.
2. **Absence is empty.** When the node does not exist in a named revision the single-revision
   functions return the empty sequence, exactly as they do for objects.
3. **Uniform across kinds.** For every stored item the result equals what the same function
   returns when the node is reached through its enclosing object or array in each revision.
   Objects and arrays keep their existing result views (object, array, array slice, key array).

## Errors

| Condition | Error |
| --- | --- |
| The argument is not read from a Sirix store: a constructed object or array, a member of one, a computed value (`upper-case(...)`, `$v + 1`) or a literal | `jn:SIRIXDBF0005`; the message names the function and the item type |
| The argument is the empty sequence or more than one item | the signature's cardinality error, `err:XPTY0004` |

No `ClassCastException` (or other Java exception) reaches the caller for any argument.

## Performance

The functions open one read-only transaction per revision they visit; that cost dominates and is
unchanged. Dispatch is a type test on the argument; value items, scans and arithmetic are not
touched.
