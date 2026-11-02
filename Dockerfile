FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_ROOT_USER_ACTION=ignore
RUN pip install --quiet "pyezvizapi==1.0.5.0" "paho-mqtt>=2.1,<3" "onnxruntime>=1.17" numpy pillow
COPY app /app
WORKDIR /app
CMD ["python", "-u", "/app/bridge.py"]
