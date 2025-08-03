1 Transaction
=============
```
johannes@luna:~/IdeaProjects/sirix$ perf stat -dd ./gradlew :sirix-core:test --tests io.sirix.service.json.shredder.JsonShredderTest.testChicagoDescendantAxis

> Configure project :
The 'sonarqube' task depends on compile tasks. This behavior is now deprecated and will be removed in version 5.x. To avoid implicit compilation, set property 'sonar.gradle.skipCompile' to 'true' and make sure your project is compiled, before analysis has started.
The 'sonar' task depends on compile tasks. This behavior is now deprecated and will be removed in version 5.x. To avoid implicit compilation, set property 'sonar.gradle.skipCompile' to 'true' and make sure your project is compiled, before analysis has started.

> Task :sirix-core:test
[0.001s][warning][pagesize] UseLargePages disabled, no large pages configured and available on the system.

Deprecated Gradle features were used in this build, making it incompatible with Gradle 9.0.

You can use '--warning-mode all' to show the individual deprecation warnings and determine if they come from your own scripts or plugins.

For more on this, please refer to https://docs.gradle.org/8.8/userguide/command_line_interface.html#sec:command_line_warnings in the Gradle documentation.

BUILD SUCCESSFUL in 1m 26s
5 actionable tasks: 1 executed, 4 up-to-date

 Performance counter stats for './gradlew :sirix-core:test --tests io.sirix.service.json.shredder.JsonShredderTest.testChicagoDescendantAxis':

          1.542,12 msec task-clock                #    0,018 CPUs utilized          
            12.912      context-switches          #    8,373 K/sec                  
               422      cpu-migrations            #  273,649 /sec                   
            57.475      page-faults               #   37,270 K/sec                  
     6.095.949.065      cpu_core/cycles/          #    3,953 G/sec                    (80,79%)
     4.359.229.598      cpu_atom/cycles/          #    2,827 G/sec                    (21,92%)
    10.249.062.183      cpu_core/instructions/    #    6,646 G/sec                    (80,79%)
     4.370.806.244      cpu_atom/instructions/    #    2,834 G/sec                    (21,92%)
     2.136.271.817      cpu_core/branches/        #    1,385 G/sec                    (80,79%)
       861.843.757      cpu_atom/branches/        #  558,870 M/sec                    (21,92%)
        51.486.612      cpu_core/branch-misses/   #   33,387 M/sec                    (80,79%)
        40.800.874      cpu_atom/branch-misses/   #   26,458 M/sec                    (21,92%)

      87,185803262 seconds time elapsed

       1,304572000 seconds user
       0,349743000 seconds sys
```


5 Transactions
==============
```
johannes@luna:~/IdeaProjects/sirix$ perf stat -dd ./gradlew :sirix-core:test --tests io.sirix.service.json.shredder.JsonShredderTest.testChicagoDescendantAxisParallel

> Configure project :
The 'sonarqube' task depends on compile tasks. This behavior is now deprecated and will be removed in version 5.x. To avoid implicit compilation, set property 'sonar.gradle.skipCompile' to 'true' and make sure your project is compiled, before analysis has started.
The 'sonar' task depends on compile tasks. This behavior is now deprecated and will be removed in version 5.x. To avoid implicit compilation, set property 'sonar.gradle.skipCompile' to 'true' and make sure your project is compiled, before analysis has started.

> Task :sirix-core:compileTestJava
Note: Some input files use or override a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
Note: Some input files use unchecked or unsafe operations.
Note: Recompile with -Xlint:unchecked for details.

> Task :sirix-core:test
[0.000s][warning][pagesize] UseLargePages disabled, no large pages configured and available on the system.

Deprecated Gradle features were used in this build, making it incompatible with Gradle 9.0.

You can use '--warning-mode all' to show the individual deprecation warnings and determine if they come from your own scripts or plugins.

For more on this, please refer to https://docs.gradle.org/8.8/userguide/command_line_interface.html#sec:command_line_warnings in the Gradle documentation.

BUILD SUCCESSFUL in 7m 8s
5 actionable tasks: 2 executed, 3 up-to-date

 Performance counter stats for './gradlew :sirix-core:test --tests io.sirix.service.json.shredder.JsonShredderTest.testChicagoDescendantAxisParallel':

          2.554,44 msec task-clock                #    0,006 CPUs utilized          
            24.998      context-switches          #    9,786 K/sec                  
             1.334      cpu-migrations            #  522,227 /sec                   
           107.296      page-faults               #   42,004 K/sec                  
     9.720.010.211      cpu_core/cycles/          #    3,805 G/sec                    (67,02%)
     7.313.018.858      cpu_atom/cycles/          #    2,863 G/sec                    (34,61%)
    14.706.998.876      cpu_core/instructions/    #    5,757 G/sec                    (67,02%)
     6.736.059.692      cpu_atom/instructions/    #    2,637 G/sec                    (34,61%)
     3.078.318.330      cpu_core/branches/        #    1,205 G/sec                    (67,02%)
     1.344.539.167      cpu_atom/branches/        #  526,353 M/sec                    (34,61%)
        72.758.448      cpu_core/branch-misses/   #   28,483 M/sec                    (67,02%)
        51.021.264      cpu_atom/branch-misses/   #   19,974 M/sec                    (34,61%)

     429,221095573 seconds time elapsed

       2,133656000 seconds user
       0,673360000 seconds sys
```
