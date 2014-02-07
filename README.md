# Camus2Hive

A simple script to automatically map new Camus partitions to Hive partitions.

Execute the script without arguments to see its usage and parameters description.

## Hive tables

You can either create your Hive tables in advance. See the create_table file for an example of Hive table definition that works with camus2hive.

Alternatively, you can also let the script create and update your Hive tables automatically, but this requires that you have a schema repo available. See the AVRO-1124 [JIRA ticket](https://issues.apache.org/jira/browse/AVRO-1124) and [patched up code](https://github.com/mate1/avro/tree/release-1.7.5-with-AVRO-1124) for more info on how to get a proper schema repo.
