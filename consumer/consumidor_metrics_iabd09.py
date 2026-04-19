import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from pymongo import MongoClient

# --- Configuración KAFKA ---
KAFKA_SERVER = "localhost:29092"
KAFKA_TOPIC = "system-metrics-topic-iabd09"
GROUP_ID = "grupo_iabd09_id"                 

# --- Configuración MONGODB ATLAS ---
MONGO_URI = "mongodb+srv://adriarreguiv_db_user:iabdiabd09@cluster0.leqjiar.mongodb.net/?appName=Cluster0"
DB_NAME = "iabd_monitoring" # Nombre de la bd
COLLECTION_RAW = "system_metrics_raw_iabd09" # Nombre de la coleccion RAW
COLLECTION_KPI = "system_metrics_kpis_iabd09" # Nombre de la coleccion KPI

# --- Configuración de Agregación ---
MENSAJES_POR_VENTANA = 20 # N = 20

if __name__ == "__main__":
    print("Iniciando Consumidor de Métricas...")
    print("-" * 30)

    # --- 1. Inicialización de Conexiones ---
    try:
        # Conexión a MongoDB
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        col_raw = db[COLLECTION_RAW]
        col_kpi = db[COLLECTION_KPI]
        
        # Comprobamos la conexión a Mongo haciendo un ping
        mongo_client.admin.command('ping')
        print("Conectado exitosamente a MongoDB Atlas.")

        # Conexión a Kafka
        consumidor = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            group_id=GROUP_ID,
            auto_offset_reset='latest', # Empezar a leer los mensajes más recientes
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Desempaquetar el JSON
        )
        print("Conectado exitosamente a Kafka. Esperando mensajes...")
        
    except Exception as e:
        print(f"Error crítico en las conexiones iniciales: {e}")
        exit(1)

    # --- 2. Variables para la Lógica de Ventanas (KPIs) ---
    mensajes_ventana = []
    inicio_ventana = time.time() # Registramos cuándo empezamos a contar los 20 mensajes

    # --- 3. Bucle Principal de Consumo ---
    try:
        # El consumidor se queda escuchando el topic indefinidamente
        for mensaje in consumidor:
            datos_metrica = mensaje.value # Aquí sacamos el diccionario de Python que envió el productor
            
            # A) Almacenar métrica bruta en MongoDB
            try:
                col_raw.insert_one(datos_metrica.copy()) # Guardamos una copia para no alterar el original
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Métrica guardada (Server: {datos_metrica['server_id']})")
            except Exception as e:
                print(f"Error al guardar métrica bruta en Mongo: {e}")
            
            # Añadimos la métrica actual a nuestra lista "ventana"
            mensajes_ventana.append(datos_metrica)

            # B) Lógica para calcular KPIs cada N=20 mensajes
            if len(mensajes_ventana) == MENSAJES_POR_VENTANA:
                fin_ventana = time.time()
                duracion_ventana = fin_ventana - inicio_ventana
                
                print(f"\n--- Alcanzados {MENSAJES_POR_VENTANA} mensajes. Calculando KPIs ---")
                
                # Inicializamos acumuladores para calcular los promedios
                suma_cpu = 0
                suma_memoria = 0
                suma_disco = 0
                suma_red = 0
                suma_errores = 0
                
                # Sumamos los valores de los 20 mensajes
                for msg in mensajes_ventana:
                    metrics = msg['metrics']
                    suma_cpu += metrics['cpu_percent']
                    suma_memoria += metrics['memory_percent']
                    suma_disco += metrics['disk_io_mbps']
                    suma_red += metrics['network_mbps']
                    suma_errores += metrics['error_count']
                
                # Calculamos los KPIs requeridos
                kpi_doc = {
                    "timestamp_calculo_utc": datetime.now(timezone.utc).isoformat(),
                    "duracion_ventana_segundos": round(duracion_ventana, 2),
                    "numero_mensajes_procesados": MENSAJES_POR_VENTANA,
                    "kpis": {
                        "promedio_cpu_percent": round(suma_cpu / MENSAJES_POR_VENTANA, 2),
                        "promedio_memory_percent": round(suma_memoria / MENSAJES_POR_VENTANA, 2),
                        "promedio_disk_io_mbps": round(suma_disco / MENSAJES_POR_VENTANA, 2),
                        "promedio_network_mbps": round(suma_red / MENSAJES_POR_VENTANA, 2),
                        "suma_errores": suma_errores,
                        "tasa_procesamiento_msg_por_seg": round(MENSAJES_POR_VENTANA / duracion_ventana, 2) if duracion_ventana > 0 else 0
                    }
                }
                
                # C) Guardar el documento KPI en MongoDB
                try:
                    col_kpi.insert_one(kpi_doc)
                    print(f"KPIs calculados y guardados exitosamente. Tasa: {kpi_doc['kpis']['tasa_procesamiento_msg_por_seg']} msg/seg\n")
                except Exception as e:
                    print(f"Error al guardar KPIs en Mongo: {e}")
                
                # Reiniciamos la ventana y el cronómetro para el siguiente bloque de 20 mensajes
                mensajes_ventana = []
                inicio_ventana = time.time()

    except KeyboardInterrupt:
        print("\nConsumo detenido por el usuario.")
    finally:
        # --- 4. Cierre adecuado de recursos ---
        print("\nCerrando conexiones...")
        if 'consumidor' in locals():
            consumidor.close()
            print("Conexión con Kafka cerrada.")
        if 'mongo_client' in locals():
            mongo_client.close()
            print("Conexión con MongoDB cerrada.")