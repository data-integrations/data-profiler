# Field Stats Batch Aggregator

Description
-----------

Calculates statistics for each input field.
For every field, a total count and null count will be calculated.
For numeric fields, min, max, mean, stddev, zero count, positive count, and negative count will be calculated.
For string fields, min length, max length, mean length, and empty count will be calculated.
For boolean fields, true and false counts will be calculated.
When calculating means, only non-null values are considered.

Properties
----------

**numPartitions:** The number of partitions to use when calculating field stats, which controls the parallelism
of the operation. Defaults to the number of fields in the input schema.

Example
-------

Suppose the input to the stage consists of the following records:

    +-----------+--------------+---------+--------------+
    | fieldName | record1      | record2 | record3      |
    +-----------+--------------+---------+--------------+
    | name      | samuel       | dwayne  | christopher  |
    | age       | 56           | 20      | 23           |
    | purchases | 10           | 50      | 0            |
    | address   | 123 Fake St. | null    | ""           |
    | isActive  | true         | false   | false        |
    +-----------+--------------+---------+--------------+

The output will be:

    +------------+---------+---------+------------+---------+----------+
    | fieldName  | record1 | record2 | record3    | record4 | record5  |
    +------------+---------+---------+------------+---------+----------+
    | field      | name    | age     | purchases  | address | isActive |
    | totalCount | 3       | 3       | 3          | 3       | null     |
    | nullCount  | 0       | 0       | 0          | 2       | null     |
    | zeroCount  | null    | 0       | 1          | null    | null     |
    | posCount   | null    | 3       | 2          | null    | null     |
    | negCount   | null    | 0       | 0          | null    | null     |
    | min        | null    | 20      | 0          | null    | null     |
    | max        | null    | 56      | 50         | null    | null     |
    | mean       | null    | 33      | 20         | null    | null     |
    | stddev     | null    | 28.20   | 31.62      | null    | null     |
    | emptyCount | 0       | null    | null       | 1       | null     |
    | lenMin     | 6       | null    | null       | 0       | null     |
    | lenMax     | 11      | null    | null       | 12      | null     |
    | lenMean    | 7.67    | null    | null       | 6       | null     |
    | lenStddev  | 4.08    | null    | null       | 6       | null     |
    | trueCount  | null    | null    | null       | null    | 1        |
    | falseCount | null    | null    | null       | null    | 2        |
    +------------+---------+---------+------------+---------+----------+
