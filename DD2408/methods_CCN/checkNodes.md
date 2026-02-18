# *checkNodes*
Path: bundles/sirix-core/src/main/java/io/sirix/diff/XmlFullDiff.java

## Cyclomatic Complexity

### Manual count
The method contains:

5 if
4 case
2 for loops
6 `&&`

1 throw
1 return

Total CC: 17 - 2 + 2 = 17

### Lizard

According to lizard, the CC is 18. This could be due to lizard not recognizing `return` or `throw` as exit points. Counting the CC with these rules, the CC would become 18.

## LOC

The method has 52 lines of code.

## Purpose
The method checks two Xml tree nodes for different node types. For example, if the node is an `Element`, it checks for several things, including matching names, attribute and namespace keys. The parameters are two read-only transactions of one old and one new revision.
