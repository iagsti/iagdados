FROM python:3.13-slim

WORKDIR /opt/dagster/app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    tor \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar uv
RUN pip install --no-cache-dir uv

# Copiar apenas arquivos de dependências
COPY pyproject.toml ./
COPY uv.lock* ./

# Instalar dependências Python
RUN uv pip install --system --no-cache .

# NÃO copiar src/ - será mapeado via volume

# Criar diretórios necessários
RUN mkdir -p /opt/dagster/dagster_home/storage/logs

# Expor portas
EXPOSE 3000 4000

# Usuário não-root (segurança)
RUN useradd -m -u 1000 dagster && \
    chown -R dagster:dagster /opt/dagster

USER dagster
