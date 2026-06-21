FROM python:3.12-slim

WORKDIR /app

COPY backend/ ./backend/
COPY scripts/ ./scripts/
COPY frontend/vendor/ ./frontend/vendor/

RUN pip install --no-cache-dir gunicorn==23.0.0

RUN mkdir -p /data

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    BACKEND_BIND=0.0.0.0 \
    PORT=8000

EXPOSE 8000

CMD ["sh", "-c", "if [ \"${XHI_PROCESS_TYPE:-web}\" = \"campaign-worker\" ]; then python scripts/run_campaign_jobs.py --all-campaigns --steps ${XHI_CAMPAIGN_JOB_STEPS:-aggregate,health,sync} --continue-on-error --loop --interval-seconds ${XHI_CAMPAIGN_JOB_INTERVAL_SECONDS:-3600}; else gunicorn --workers ${WEB_CONCURRENCY:-1} --threads ${WEB_THREADS:-8} --timeout ${WEB_TIMEOUT:-120} --access-logfile - --error-logfile - --bind 0.0.0.0:${PORT:-8000} backend.wsgi:application; fi"]
