# iterateAxis
## Function

### Path
`bundles/sirix-saxon/src/main/java/org/sirix/saxon/wrapper/NodeWrapper.java`

### Method declaration
```java
public AxisIterator iterateAxis(final byte axisNumber, final NodeTest nodeTest)
``` 

## Cyclomatic Complexity

### Lizard
30

### Manual count
8 `if`
20 `case`
1 `catch`

1 `return`
1 `throw`


Total: pi - s + 2 = 29 - 2 + 2 = 29

This might differ since Lizard might not recognize this function as multi-exit with the throw and return.
If we recognize this function as having 1 exit then the total would instead become 30.

pi + 1 = 29 + 1 = 30

## Code Length
In terms of LOC the function is quite long with lizard reporting LOC of 113.

## Purpose
The purpose of the function is to be a iterator factory, depending on what axis is passed in, the corresponding iterator is constructed and returned. Since different axis require different constructions a giant switch case is used which increases the cyclomatic complexity by quite alot.
