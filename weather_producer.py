from confluent_kafka import Producer
import json
import requests
import time
import os

# Configuration de l'API météo
API_KEY = os.getenv('WEATHER_API_KEY', '5b04c0a38a844bb8bbf101004243010')
BASE_URL = 'http://api.weatherapi.com/v1/current.json'

# Liste des villes en France
cities = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"]

# Configuration de Kafka
producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

# Fonction pour obtenir les données météo pour une ville
def get_weather_data(city):
    url = f"{BASE_URL}?key={API_KEY}&q={city}&aqi=no"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erreur de l'API météo pour {city} :", e)
        return None

# Fonction de rappel pour gérer les erreurs Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Échec de l'envoi de message pour {msg.key()}: {err}")
    else:
        print(f"Message envoyé pour {msg.key()}: {msg.value()}")

# Production de messages pour chaque ville toutes les 15 minutes
while True:
    start_time = time.time()  # Heure de début de l'itération
    
    for city in cities:
        weather_data = get_weather_data(city)
        if weather_data:
            # Ajouter le nom de la ville aux données
            weather_data['city'] = city
            try:
                # Envoi du message Kafka
                producer.produce(
                    'weather-data',
                    key=city.encode('utf-8'),
                    value=json.dumps(weather_data).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
            except Exception as e:
                print(f"Erreur lors de l'envoi des données pour {city}: {e}")
        
        # Pause courte pour éviter de surcharger l'API
        time.sleep(1)  # Ajustable si nécessaire pour respecter le rate limit
    
    # Pause restante pour atteindre un intervalle total de 15 minutes
    elapsed_time = time.time() - start_time
    sleep_time = max(0, 900 - elapsed_time)  # 900 secondes = 15 minutes
    print(f"Attente de {sleep_time / 60:.2f} minutes avant la prochaine collecte.")
    time.sleep(sleep_time)
