# udacity_sanfran_crime
How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

maxOffsetsPerTrigger: This setting impacted throughput; that is, the inputRowsPerSecond to processedRowsPerSecond ratio. In general, I found increasing the value of maxOffsetsPerTrigger increased the ratio of inputRowsPerSecond to processedRowsPerSecond.

spark.sql.shuffle.partitions: This setting controls the number of shuffle partitions during group or join operation. Effective parallelizing could positively affect the throughput and latency.

The two properties that affected efficiency the most were:

In my test, modifying "maxOffsetsPerTrigger" and "maxRatePerPartition" properties was most efficient properties.

I tested some of the default parameters for spark.sql.shuffle.partitions, partition, maxRateperPartition and maxOffsetPerTriger. I also tested different values to understand how it varied and found the best values were maxRatePerPartition: 300 and maxOffserPerTrigger: 200. This combination increased inputRowsPerSecond to processedRowsPerSecond ratio the best.
