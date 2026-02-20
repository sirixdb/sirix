# Report for assignment 3

This is a template for your report. You are free to modify it as needed.
It is not required to use markdown for your report either, but the report
has to be delivered in a standard, cross-platform format.

## P+
The following people are going for P+:
- Melker Trané (processNode)

## Project

Name: SirixDB

URL: [Sirix github repo](https://github.com/sirixdb/sirix)

Purpose: The sirix database has the main goal of handling the history of a database in a neat way.

## Onboarding experience

The project could easily be built by cloning the repo and following the instructions in the project's readme. 
The required tools to build the project is Java 25 or later, and gradle 9.1 or later. However, gradle was not
necessary to install since you could use the wrapper, and therefore build using ```./gradlew test```. Other tools,
such as plugins, was installed automatically by the build script.

The build sometimes conclude with one error in the tests, and several tests are ignored or skipped. Additionally,
the build can sometimes fail due to different causes. Otherwise, building the project with ```./gradlew build -x test```
succeeds.

## Complexity

### 1. What are your results for five complex functions?

| Method        | NLOC (Lizard) | CCN (Lizard) | CCN (Manual) |
| :-----------: | :-----------: | :-----------:| :----------: |
| `serialize`   | 62            | 16           | 16           |
| `iterateAxis` | 113           | 30           | 29           |
|`isNCStartChar`| 11            | 24           | 26           |
|`getReturnType`| 60            | 25           | 11           |
| `processNode` | 122           | 48           | 40           |

### 2. Are the functions just complex, or also long?

As seen in the table above, function length does not always correlate with complexity. The function `isNCStartChar` had a high complexity of 24-26 with only having 11 NLOC. Since functions exceeding 50 NLOC are generally considered long, the other five functions are would be categorized as long as well. All of them also achieve high cyclomatic complexity. 

### 3. What is the purpose of the functions?

#### `serialize`
The purpose of the method `serialize` is to act like a central component for transforming a `Sequence` of query results and convert them into JSON output. It is used when query results are to be presented to users or external systems, and to ensure that data types are serialized into valid JSON. The method first checks if the current result is the first and initializes a JSON "rest" array. It further iterates over the items in the Sequence, and each element is serialized according to its type, whether that's arrays, objects, structured database nodes, or atomic values.

#### `iterateAxis`
The purpose of the function is to be a iterator factory, depending on what axis is passed in, the corresponding iterator is constructed and returned. Since different axis require different constructions a giant switch case is used which increases the cyclomatic complexity by quite alot.

#### `isNCStartChar`
The purpose of the function is validate whether a character input is a valid XML non colonised name start character according to XML specification. It also checks multiple unicode ranges. As the XML spec requirements check over 20 different specific character ranges and each range check itself adds to the CC, this causes high cyclomatic complexity.

#### `getReturnType`
The function takes two operands as arguments, and returns the corresponding type for the sub operation. If the operands are not compatible, it throws errors. 

#### `processNode`
The purpose of the method is to take a read only JSON node object and insert a copy of that node in a specific location in a writeable JSON object.

### 4. Are exceptions taken into account in the given measurements?

Lizard did not seem to take multiple exit points or exceptions (throws) into account when measuring the CCN. When adjusting for this, we seem to get the same resulsts as Lizard.

## Refactoring

TODO: Need to add for all functions

### `processNode`
The high complexity is not needed. We can split the function into smaller parts which each take care of a insert location.

#### P+ Implementation (Melker Trané)
For an implimentation of the refactor, see branch refactor-melker and file bundles/sirix-core/src/main/java/io/sirix/service/json/shredder/JsonResourceCopy.java

The processNode function now has a CCN of 4 while the 3 helper functions each has a CCN of 14.

## Coverage

For our ad hoc coverage tool we have a CoverageRegister singleton. This singleton has fixed length array of boolean values where the value at a certain position correspond to whether a certain branch has been covered or not. In every branch we add a line which "registers" that branch as visited (eg. 'CoverageRegister.register(3)' for the forth branch). When all tests are executed we call a 'getReport' method on the singleton which returns a string with contains information on what branches were covered and what branches were not.

## Coverage improvement

The branches that contain the improvements is called "improved-coverage"
 
Below is a table that shows the branch coverage for the different methods before and after the tests were added. 

| Method        | Current       | Improved     | Extra (P+)   |
| :-----------: | :-----------: | :-----------:| :----------: |
| `serialize`   |               |              |              |
| `iterateAxis` |               |              |              |
|`isNCStartChar`|               |              |              |
|`getReturnType`|               |              |              |
| `processNode` | 10            | 17           | 27           |

### processNode

The method has two "input" values which determine which branch is visited. The first value is where we should insert. The other is what type we are inserting. Depedning on the pair of these values, a specific branch is visited which calls a specific method (for example insertBooleanAsFirstChild or insertNumberAsLastChild).

To improve the coverage we made two new tests: one which tests all pairs where we insert as left sibling, and another where we tests all pairs where we insert as right sibling.

The assertions checks if the writable and readable databaseses looks the same after the insertions are made.

#### Extra tests for P+ (Melker Trané)
Two extra tests was made. The first tests every pair which inserts as first child, and the second tests every pair which inserts as last child.

When inserting as last child, we assert that we throw exceptions since this is not supported by the 'processNode' function.

See branch extra-coverage-melker and the file bundles/sirix-core/src/test/java/io/sirix/service/json/shredder/JsonResourceCopyTest.java

## Self-assessment: Way of working

As in the first assignment, we still consider ourselves to be in the "in use" state. This is mainly because this assignment was quite differnt from the first two, which required us to form new practices and foundations. And because of the relative short time period between this and the last assignment, we have not spent that much time to fully flesh out all practices for this assignment. While we have gotten to know each other better, our commucation has improved. For the next assignment, which seems to more like this one compared to the first two, we hope to have a good plan going into the assignment, so we do not have to spend as much time to change/redo stuff when different approaches are taken by different team members.
