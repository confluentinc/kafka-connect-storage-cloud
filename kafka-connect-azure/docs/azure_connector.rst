Azure Connector
==============

The Azure connector, currently available as a sink, allows you to export data from Kafka topics to Azure Blob objects in
either Avro or JSON formats. In addition, for certain data layouts, Azure connector exports data by guaranteeing
exactly-once delivery semantics to consumers of the Azure objects it produces.

Being a sink, the Azure connector periodically polls data from Kafka and in turn uploads it
to Azure. A partitioner is used to split the data of every Kafka partition into chunks. Each chunk of data is
represented as an Azure object, whose key name encodes the topic, the Kafka partition and the start offset of
this data chunk. If no partitioner is specified in the configuration, the default partitioner which
preserves Kafka partitioning is used. The size of each data chunk is determined by the number of
records written to Azure Blob and by schema compatibility.



