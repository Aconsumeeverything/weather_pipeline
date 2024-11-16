from kafka import KafkaConsumer
import json

# Configuration du consommateur Kafka
consumer = KafkaConsumer(
    'weather-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',  # Pour lire les messages depuis le début
    enable_auto_commit=True,
    group_id='weather-group'
)

# Boucle pour consommer les messages
for message in consumer:
    weather_data = message.value
    print(f"Données reçues : {weather_data}")
    # Appeler une fonction de traitement ici
