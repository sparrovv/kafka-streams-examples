# kafka streams examples

This repository contains a few examples I used in my kafka streams presentation on how to mix Processor API with streams DSL.

Examples 1-3 contain basic kafka streams operations.

Examples 4-6 show how to implement a contrived leads management system with:

- KStream - GlobalTable joins
- KeyValue stores to persist objects for deduplication
- WallClock scheduler to delay sending messages


All examples are tested with kafka-streams-test-utils.


[Link to youtube presentation](https://www.youtube.com/watch?v=UKF9-l16lww&list=PL9l6mdcA_0l1jLuWzChvdKm4qjOIwjPd0)
