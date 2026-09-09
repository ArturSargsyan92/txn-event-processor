"""Redis Streams transport: connection, producer, and consumer-group consumer.

Note this package shadows the stdlib `queue` only for relative lookups; absolute imports are
the default in Python 3, so `import queue` elsewhere still resolves to the standard library.
"""
