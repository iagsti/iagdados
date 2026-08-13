FROM python:3.13-slim

WORKDIR /opt/dagster/app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    tor \
    curl \
    build-essential \
    python3-dev \
    libmariadb-dev \
    pkg-config \
    libpq-dev \
    libxml2-dev \
    libxslt-dev \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar uv
RUN pip install --no-cache-dir uv

# Copiar apenas arquivos de dependências
COPY pyproject.toml ./
COPY uv.lock* ./

# Instalar dependências Python
RUN uv pip install --system --no-cache .
RUN uv sync

# Código da aplicação embutido na imagem (não depende de bind mount/estado do host em produção)
COPY src/ /opt/dagster/app/src/

# Criar diretórios necessários
RUN mkdir -p /opt/dagster/dagster_home/storage/logs && \
    mkdir -p /opt/dagster/data

# Config fixa do Dagster (instance config + workspace) embutida na imagem,
# fora do caminho do volume. O entrypoint sincroniza isso para dentro do
# DAGSTER_HOME a cada start, então não depende do volume estar vazio.
COPY dagster_home/dagster.yaml /opt/dagster/config/dagster.yaml
COPY dagster_home/workspace.yml /opt/dagster/config/workspace.yml

COPY entrypoint.sh /opt/dagster/entrypoint.sh
RUN chmod +x /opt/dagster/entrypoint.sh

# Expor portas
EXPOSE ${WEBSERVER_PORT} ${DAEMON_PORT}

# Usuário não-root (segurança)
RUN useradd -m -u 1000 dagster && \
    chown -R dagster:dagster /opt/dagster

USER dagster

ENTRYPOINT ["/opt/dagster/entrypoint.sh"]
