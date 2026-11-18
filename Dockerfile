FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY relay ./relay
COPY upstream ./upstream
COPY README.md ./

EXPOSE 8080
CMD ["python", "-m", "relay.main"]
