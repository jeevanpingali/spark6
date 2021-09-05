# Spark read and write - with and without Executor Framework

Trying to see what happens if we can improve performance, by introducing executor framework into already parallel Spark execution and the results are interesting. (These tests are done in my laptop, from IntelliJ Idea)

## 1000 writes
### With Executor:
* Started: Sun Sep 05 18:39:02 IST 2021
* Completed: Sun Sep 05 18:40:26 IST 2021
* Took 1 minute 24 seconds

### Without Executor:
* Started: Sun Sep 05 18:40:58 IST 2021
* Completed: Sun Sep 05 18:45:15 IST 2021
* Took 4 minutes 17 seconds

## 10000 Writes

### With Executor:
* Started: Sun Sep 05 19:26:22 IST 2021
* Completed: Sun Sep 05 19:50:00 IST 2021
* Took 23 minutes 38 seconds

### Without Executor:
* Started: Sun Sep 05 19:54:54 IST 2021
* Completed: Sun Sep 05 20:39:48 IST 2021
* Took 44 minutes 54 seconds
