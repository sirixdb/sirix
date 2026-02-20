# processNode
Path: /bundles/sirix-core/src/main/java/io/sirix/service/json/shredder/JsonResourceCopy.java

## LOC
According to Lizard the function had 122 lines of code.

## Cyclomatic Complexity

### Lizard
Lizard gives a CCN of 48.

### Manual count
Number of branches:
if: 7
else if: 14
case: 26
catch: 0
while: 0
for: 0
||: 0
&&: 0
total: 47

Number of exits:
return: 1 (implicit)
throw: 8
total: 9

We get a CCN of 2 + 47 - 9 = 40, which is eight less than what Lizard provided. This could be explained if Lizard does not count "throw" as a real exit point.

## Purpose
The purpose of the method is to take a read only JSON node object and insert a copy of that node in a specific location in a writeable JSON object.

## Coverage
The current branch coverage is 10/30. The method has two values which both decide which branch is visited. The first value is where we should insert. The other is what type we are inserting. Depedning on which pair of values we have we need to call a specific method. (for example insertBooleanAsFirstChild or insertNumberAsLastChild)

To improve the coverage I made four new tests where each test aims to cover a certain location to insert to (First child, Last child, left sibling, right sibling). Every test contains nodes of every type so we can fully cover all occurances of a certain location.

The improved branch coverage is 27/30. See branch process-node-coverage-improvement

## Refactoring plan
The high complexity is not needed. We can split the function into smaller parts which each take of on insert location.

For an implimentation of the refactor, see branch process-node-refactor. The processNode function now has a CCN of 4 while the 3 helper functions each has a CCN of 14.

