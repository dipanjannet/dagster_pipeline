FROM python:3.11

WORKDIR /app

COPY . /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir uv && uv pip install --system -r requirements.txt

ENV DAGSTER_HOME=/app/dagster_home

RUN mkdir -p /app/dagster_home

RUN mkdir -p /workspace/dagster_pipeline/data_pipeline/data

RUN touch /app/dagster_home/dagster.yaml

EXPOSE 3000

CMD ["dagster", "dev", "--host", "0.0.0.0"]
