FROM ghcr.io/astral-sh/uv:0.9.17 AS uv

FROM apache/airflow:2.10.5-python3.11

USER root
# Runtime deps for snscrape/telegram web scraping
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# uv for reproducible Python dependency management (pyproject.toml + uv.lock)
COPY --from=uv /uv /uvx /usr/local/bin/

USER airflow

# Locked project environment inside the image; snscrape is fetched from git via the lock.
# PyPI index access is enough; no build of the project package itself (package = false).
COPY pyproject.toml uv.lock /tmp/env/
WORKDIR /tmp/env
# CPU-only torch first (the PyPI wheel for linux/amd64 drags in ~1.5 GB of
# unused CUDA/NVIDIA libraries; inference runs on CPU). Plain torch==<ver>
# from the lock is then satisfied by the already-installed +cpu build.
# torch/triton/nvidia-* строки вырезаются: torch уже установлен из cpu-индекса,
# а CUDA-зависимости PyPI-колеса torch для CPU-inference не нужны.
COPY <<'EOF' /tmp/strip.awk
/^torch==/ || /^triton==/ || /^nvidia-/ {f=1; next}
f && /^[^ ]/ {f=0}
!f {print}
EOF
RUN TORCH_VER=$(grep -oP '^torch==\K[0-9.]+' <(uv export --frozen --no-dev --no-emit-project)) \
    && export UV_HTTP_TIMEOUT=600 \
    && uv pip install --no-cache --python "$(which python)" \
        "torch==${TORCH_VER}" --index-url https://download.pytorch.org/whl/cpu \
    && uv export --frozen --no-dev --no-emit-project \
        --output-file /tmp/requirements.txt \
    && awk -f /tmp/strip.awk /tmp/requirements.txt > /tmp/requirements2.txt \
    && mv /tmp/requirements2.txt /tmp/requirements.txt \
    && uv pip install --no-cache --python "$(which python)" -r /tmp/requirements.txt

# Project code will be mounted as a volume; nothing else to copy.
ENV PYTHONPATH=/opt/airflow/src:${PYTHONPATH}
