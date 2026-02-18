# *serialize*
Path: bundles/sirix-query/src/main/java/io/sirix/query/JsonDBSerializer.java

## Cyclomatic Complexity
According to *Lizard*, the method `serialize` has 62 NLOC and a CCN of 16. The CCN was calculated by using the decision point formula defined as 1 + decision points (e.g., if-statments, loops, catch-block). This also resulted in a CCN of 16. 

## Purpose
The purpose of the method `serialize` is to act like a central component for transforming a `Sequence` of query results and convert them into JSON output. It is used when query results are to be presented to users or external systems, and to ensure that data types are serialized into valid JSON. The method first checks if the current result is the first and initializes a JSON "rest" array. It further iterates over the items in the Sequence, and each element is serialized according to its type, whether that's arrays, objects, structured database nodes, or atomic values.