from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime
import json

# Tüm illerin enlem ve boylam bilgileri
LOCATIONS = [
    {'latitude': '37.0010', 'longitude': '35.3213'},  # Adana
    {'latitude': '39.7191', 'longitude': '43.0514'},  # Ağrı
    {'latitude': '40.9833', 'longitude': '27.5167'},  # Edirne
    
    {'latitude': '40.1826', 'longitude': '29.0665'},  # Bursa
    {'latitude': '38.4192', 'longitude': '27.1287'},  # İzmir
    {'latitude': '39.9208', 'longitude': '32.8541'},  # Ankara
    {'latitude': '41.0082', 'longitude': '28.9784'},  # İstanbul
    {'latitude': '37.7648', 'longitude': '30.5567'},  # Isparta
    {'latitude': '36.8841', 'longitude': '30.7056'},  # Antalya
    """
    {'latitude': '37.8667', 'longitude': '32.4833'},  # Konya
    {'latitude': '38.6742', 'longitude': '39.2232'},  # Elazığ
    {'latitude': '38.6270', 'longitude': '34.7120'},  # Nevşehir
    {'latitude': '41.4564', 'longitude': '31.7987'},  # Zonguldak
    {'latitude': '37.7833', 'longitude': '29.0947'},  # Denizli
    {'latitude': '41.0015', 'longitude': '39.7178'},  # Trabzon
    {'latitude': '37.7648', 'longitude': '38.2766'},  # Malatya
    {'latitude': '40.1467', 'longitude': '29.9793'},  # Yalova
    {'latitude': '38.4192', 'longitude': '27.1287'},  # Manisa
    {'latitude': '41.2867', 'longitude': '36.33'},    # Samsun
    {'latitude': '41.0170', 'longitude': '39.5269'},  # Rize
    {'latitude': '39.7767', 'longitude': '30.5206'},  # Eskişehir
    {'latitude': '39.9167', 'longitude': '44.0333'},  # Iğdır
    {'latitude': '38.3739', 'longitude': '38.3885'},  # Sivas
    {'latitude': '40.8471', 'longitude': '31.3754'},  # Düzce
    {'latitude': '37.0150', 'longitude': '37.3219'},  # Gaziantep
    {'latitude': '41.6434', 'longitude': '32.3338'},  # Bartın
    {'latitude': '37.0170', 'longitude': '37.3219'},  # Kilis
    {'latitude': '40.1553', 'longitude': '29.0665'},  # Bilecik
    {'latitude': '40.6013', 'longitude': '33.6134'},  # Çankırı
    {'latitude': '41.0667', 'longitude': '37.3833'},  # Sinop
    {'latitude': '37.5744', 'longitude': '36.9371'},  # Kahramanmaraş
    {'latitude': '38.4944', 'longitude': '43.3833'},  # Van
    {'latitude': '40.6167', 'longitude': '43.1000'},  # Kars
    {'latitude': '39.7667', 'longitude': '37.0167'},  # Tokat
    {'latitude': '38.6738', 'longitude': '39.2264'},  # Bingöl
    {'latitude': '41.3793', 'longitude': '33.7753'},  # Karabük
    {'latitude': '40.9833', 'longitude': '27.5167'},  # Tekirdağ
    {'latitude': '41.6843', 'longitude': '26.5623'},  # Kırklareli
    {'latitude': '37.5744', 'longitude': '43.7333'},  # Hakkâri
    {'latitude': '41.1419', 'longitude': '40.5294'},  # Artvin
    {'latitude': '37.7619', 'longitude': '30.5523'},  # Burdur
    {'latitude': '41.3201', 'longitude': '36.3194'},  # Ordu
    {'latitude': '38.6025', 'longitude': '27.0285'},  # Uşak
    {'latitude': '37.1810', 'longitude': '33.2150'},  # Karaman
    {'latitude': '38.3750', 'longitude': '42.1106'},  # Bitlis
    {'latitude': '37.5697', 'longitude': '42.4822'},  # Şırnak
    {'latitude': '39.6511', 'longitude': '27.8861'},  # Balıkesir
    {'latitude': '41.1993', 'longitude': '32.6270'},  # Zonguldak
    {'latitude': '38.3903', 'longitude': '34.0284'},  # Aksaray
    {'latitude': '40.5986', 'longitude': '31.5788'},  # Bolu
    {'latitude': '39.8569', 'longitude': '41.2769'},  # Erzurum
    {'latitude': '40.5489', 'longitude': '34.9533'},  # Çorum
    {'latitude': '37.2153', 'longitude': '28.3636'},  # Muğla
    {'latitude': '40.8533', 'longitude': '29.8814'},  # Kocaeli
    {'latitude': '40.2266', 'longitude': '29.0093'},  # Sakarya
    {'latitude': '37.7648', 'longitude': '38.2766'},  # Adıyaman
    {'latitude': '38.7636', 'longitude': '30.5406'},  # Afyonkarahisar
    {'latitude': '40.6531', 'longitude': '35.8330'},  # Amasya
    {'latitude': '41.1105', 'longitude': '42.7022'},  # Ardahan
    {'latitude': '40.2552', 'longitude': '40.2249'},  # Bayburt
    {'latitude': '40.1553', 'longitude': '26.4142'},  # Çanakkale
    {'latitude': '37.9144', 'longitude': '40.2306'},  # Diyarbakır
    {'latitude': '39.7500', 'longitude': '39.5000'},  # Erzincan
    {'latitude': '40.9128', 'longitude': '38.3895'},  # Giresun
    {'latitude': '40.4600', 'longitude': '39.4800'},  # Gümüşhane
    {'latitude': '36.2025', 'longitude': '36.1606'},  # Hatay
    {'latitude': '41.3767', 'longitude': '33.7764'},  # Kastamonu
    {'latitude': '39.8468', 'longitude': '33.5153'},  # Kırıkkale
    {'latitude': '39.1467', 'longitude': '34.1608'},  # Kırşehir
    {'latitude': '37.3122', 'longitude': '40.7350'},  # Mardin
    {'latitude': '38.7322', 'longitude': '41.4911'},  # Muş
    {'latitude': '37.9667', 'longitude': '34.6833'},  # Niğde
    {'latitude': '37.0742', 'longitude': '36.2478'},  # Osmaniye
    {'latitude': '37.9333', 'longitude': '41.9500'},  # Siirt
    {'latitude': '37.1591', 'longitude': '38.7969'},  # Şanlıurfa
    {'latitude': '39.1083', 'longitude': '39.5472'},  # Tunceli
    {'latitude': '39.8200', 'longitude': '34.8044'},  # Yozgat"""
]

# Airflow bağlantı ID'leri
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

# DAG için varsayılan argümanlar
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# DAG tanımı
with DAG(
    dag_id='weather_etl_pipeline_all_locations',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    @task()
    def extract_weather_data(location):
        """Open-Meteo API'den hava durumu verilerini çeker."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f"/v1/forecast?latitude={location['latitude']}&longitude={location['longitude']}&current_weather=true"
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data, location):
        """API'den alınan ham veriyi dönüştürür."""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': location['latitude'],
            'longitude': location['longitude'],
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """Dönüştürülmüş veriyi PostgreSQL veritabanına yükler."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Tablo yoksa oluştur
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP
        );
        """)

        # Veriyi ekle
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'],
            transformed_data['timestamp']
        ))

        conn.commit()
        cursor.close()

    # Tüm iller için ETL işlemi
    for location in LOCATIONS:
        weather_data = extract_weather_data(location)
        transformed_data = transform_weather_data(weather_data, location)
        load_weather_data(transformed_data)