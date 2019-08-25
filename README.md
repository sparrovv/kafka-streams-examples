# kafka streams examples

This repository contains a few examples I used in my kafka streams presentation on how to mix Processor API with streams DSL.

Examples 1-3 contain basic kafka streams operations.

Examples 4-6 shows how to implement a contrived leads management system with:

- Stream - GlobalTable joins
- KeyValue stores to persist objects for deduplication
- WallClock scheduler to delay sending messages


All examples are tested with kafka-streams-test-utils.
