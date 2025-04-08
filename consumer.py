import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
from flask import Flask
from threading import Thread

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv('SERVER_BOOTSTRAP')
KAFKA_PROTOCOL = os.getenv('SECURITY_PROTOCOL')
KAFKA_MECHANISM = os.getenv('SASL_MECHANISM')
KAFKA_USERNAME = os.getenv('SASL_USERNAME')
KAFKA_PASSWORD = os.getenv('SASL_PASSWORD')
DB_URI = os.getenv('DATABASE_URL')


app = Flask(__name__)

@app.route('/')
def index():
    return "Consumer is running..."

def run_flask():
    app.run(host='0.0.0.0', port=5000)


def consume_messages():
    print('Conectando a PostgreSQL...')
    try:
        conn = psycopg2.connect(DB_URI)
        cur = conn.cursor()
        print("✅ PostgreSQL conectado correctamente.")
    except Exception as e:
        print(f"❌ No se pudo conectar a PostgreSQL: {e}")
        exit(1)

    consumer = KafkaConsumer(
        'spotify',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol=KAFKA_PROTOCOL,
        sasl_mechanism=KAFKA_MECHANISM,
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    for msg in consumer:
        try:
            record = msg.value

            if isinstance(record, dict) and "0" in record:
                raw_json = record["0"]
            else:
                raw_json = record

            track_title = raw_json.get("track_title", "Unknown")
            artists = raw_json.get("artists", "Unknown")
            duration_ms = int(raw_json.get("duration_ms", 0))

            sql = "INSERT INTO tracks (track_title, artists, duration_ms) VALUES (%s, %s, %s)"
            cur.execute(sql, (track_title, artists, duration_ms))
            conn.commit()

            print(f"✅ Insertado: {track_title} - {artists} ({duration_ms}ms)")
        except json.JSONDecodeError:
            print("❌ Error: JSON inválido")
        except KeyError as e:
            print(f"❌ Clave faltante en JSON: {e}")
        except Exception as e:
            print(f"❌ Error al insertar en PostgreSQL: {e}")

    consumer.close()
    cur.close()
    conn.close()
    print("🔒 Conexión cerrada")

if __name__ == "__main__":
    flask_thread = Thread(target=run_flask)
    flask_thread.start()


    consume_messages()
