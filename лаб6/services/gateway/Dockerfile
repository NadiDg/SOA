FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common_obs.py .
COPY app.py .
COPY static ./static
ENV SERVICE_NAME=gateway-service
ENV OBS_ROLE=coordinator
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
