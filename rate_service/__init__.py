"""A stand-in FX rate service, kept deliberately separate from `app`.

It exists to be broken on demand: stopping this container (or toggling its fault switch) is how
the retry, backoff and dead-letter paths get demonstrated against a real network boundary rather
than a mock. It never imports from `app` — it is a different deployable that happens to share a
repository.
"""
