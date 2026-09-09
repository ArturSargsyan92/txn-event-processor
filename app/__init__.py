"""Async transaction-event processing service.

Two processes share this package: the FastAPI ingest/read API (`app.main`) and the
Redis Streams consumer (`app.worker`).
"""
