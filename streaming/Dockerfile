# Bitnami moved pinned-version (non-"latest") image tags to bitnamilegacy/* in
# their 2025 catalog restructure; bitnami/spark:3.5.1 itself 404s.
FROM bitnamilegacy/spark:3.5.1

USER root
WORKDIR /app

COPY streaming/requirements.txt .
# --force-reinstall: this base image ships its own (Python 3.8 venv) copy of
# pyspark on a PYTHONPATH that's visible during this RUN step but gets
# overwritten by our own PYTHONPATH below — without it, pip sees a same-version
# pyspark already "installed" there and skips installing our Python-3.11-compatible
# copy into site-packages, so it's gone once our PYTHONPATH takes over.
RUN pip install --no-cache-dir --force-reinstall -r requirements.txt

COPY streaming/src/ ./src/
COPY streaming/warmup.py .
COPY scripts/seed_lookup.py ./scripts/seed_lookup.py

# Pre-resolve Delta + Kafka connector packages (and their transitive deps) into
# the image's Ivy cache at build time, so containers never hit the network on
# startup and never race each other over a shared cache download.
RUN python warmup.py

ENV DELTA_DATA_ROOT=/data \
    SPARK_CHECKPOINT_ROOT=/checkpoints \
    PYTHONPATH=/app/src

CMD ["python", "src/main.py"]
