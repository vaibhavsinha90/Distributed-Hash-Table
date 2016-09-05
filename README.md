# Distributed-Hash-Table

Implementation of a simple DHT based on simplified Chord. It does not handle node leaves/failures. There are three main pieces: 

1) ID space partitioning/re-partitioning

2) Ring-based routing

3) Node joins

The content provider contains all DHT functionalities and supports insert and query operations. Upon running multiple instances of the app, all content provider instances form a Chord ring and serve insert/query requests in a distributed fashion according to the Chord protocol.
