# Distrubutied-systems-cw-
Developed a distributed file storage system in Java featuring one Controller and multiple Dstores. The system supports concurrent client requests (store, load, list, remove) and replicates each file R times across different Dstores to ensure availability and fault tolerance.

The Controller manages indexing, file allocation, and rebalancing when Dstores join or fail, while clients retrieve files directly from Dstores for scalability. This design highlights core concepts in networking, concurrency, and distributed systems.
