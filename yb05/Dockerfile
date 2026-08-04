FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/logs

# Application console : pas de serveur, pas de port exposé.
# La sous-commande est passée à l'exécution : docker compose run --rm yb05 db-info
ENTRYPOINT ["python", "-m", "app.main"]
CMD ["db-check"]
