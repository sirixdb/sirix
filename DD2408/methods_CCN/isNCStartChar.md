# Function: XMLToken: isNCStartChar

## Path
`bundles\sirix-core\src\main\java\io\sirix\utils\XMLToken.java`

## Method declaration
```java
public static boolean isNCStartChar(final int ch) 
``` 
## Cyclomatic Complexity

### Lizard
24

### Manual count
1 base
10 `||`/or operators
13 `&&`/and operators
2 ?: (ternary)

Total: 1+10+13+2 = 26

The manual count is 2 more than lizard due to a few reasons.
Lizard may count 2 consecutive && as 1 count or use different count system for nested loops.

## Code Length
The function is short with 11 NLOC.

## Purpose
The purpose of the function is validate whether a character input is a valid XML non colonised name start character according to XML specification. It also checks multiple unicode ranges. As the XML spec requirements check over 20 different specific character ranges and each range check itself adds to the CC, this causes high cyclomatic complexity.