FROM python:3.9-slim

# Installer les dépendances
RUN pip install requests kafka-python confluent-kafka

# Copier le fichier de script
COPY weather_producer.py /app/weather_producer.py

# Exécuter le script
CMD ["python", "/app/weather_producer.py"]
