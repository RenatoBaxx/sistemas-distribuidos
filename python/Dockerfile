FROM python:3.12-slim

WORKDIR /app

COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# gera o mensagem_pb2.py a partir do .proto
COPY proto/mensagem.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. mensagem.proto

COPY python/*.py ./

ENV PYTHONUNBUFFERED=1
