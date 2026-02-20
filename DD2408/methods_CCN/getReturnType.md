# getReturnType
## Function (Edwin's)

### Path
`bundles/sirix-core/src/main/java/io/sirix/service/xml/xpath/operators/SubOpAxis.java`

### Method declaration
```java
  protected Type getReturnType(final int mOp1, final int mOp2) throws SirixXPathException
``` 

## Cyclomatic Complexity


### Manual count
#### Decisions
12 `if`
5 `case`
1 `catch`
6 `&&` and `||`

#### Exit points
12 `return`
3 `throw`


Total: 24 - 15 + 2 = 11

### Lizard
lizard's CCN is 25

The difference is probably that lizard does not count returns or throws. Additionally, lizard seems to count the last `default` int the switch statement. Using these rules, the CCN would be 24 - 1 + 2 = 25


## NLOC
The method has 60 NLOC according to lizard.

## Purpose
The function takes two operands as arguments, and returns the corresponding type for the sub operation. If the operands are not compatible, it throws errors. 

## Refactoring plan
The method could be split into mainly three methods. The current method, a method for numerical types and a third method for other types. This would split the main body of the function and therefore reduce it's structural complexity. 

## DIY code 

## Tests
