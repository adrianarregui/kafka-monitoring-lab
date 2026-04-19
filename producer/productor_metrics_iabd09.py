import random
import time
from datetime import datetime, timezone
import uuid
import json
from kafka import KafkaProducer # Importamos la librería de Kafka

# --- Configuración ---
SERVER_IDS = ["web01", "web02", "db01", "app01", "cache01"]
REPORTING_INTERVAL_SECONDS = 10 # Tiempo entre reportes completos de todos los servers

# --- Configuración KAFKA (AÑADIDO) ---
KAFKA_TOPIC = "system-metrics-topic-iabd09" # El "buzón" o canal de Kafka donde dejaremos los mensajes
KAFKA_SERVER = "localhost:29092" # La dirección IP y el puerto donde está encendido nuestro Kafka

# --- Inicio de la Lógica de Generación (Parte A, Paso 2) ---
if __name__ == "__main__":
    print("Iniciando simulación de generación de métricas...")
    print(f"Servidores simulados: {SERVER_IDS}")
    print(f"Intervalo de reporte: {REPORTING_INTERVAL_SECONDS} segundos")
    print("-" * 30)
    
    # --- Inicialización del Productor de Kafka (AÑADIDO) ---
    try:
        # Creamos el productor. Le decimos dónde está el servidor Kafka y cómo empaquetar los datos.
        productor = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            # Esta línea coge nuestro diccionario de Python, lo convierte a texto JSON y lo codifica en bytes (que es el idioma que entiende Kafka)
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Conectado exitosamente a Kafka.")
    except Exception as e:
        # Si Kafka está apagado o hay un error de red, lo capturamos aquí
        print(f"Error al conectar con Kafka: {e}")
        exit(1) 

    # --- Inicio de la Lógica de Generación ---
    try:
        # Bucle infinito: el programa repetirá esto para siempre hasta que lo paremos
        while True:
            print(f"\n{datetime.now()}: Generando reporte de métricas...")
            
            # Pasamos lista: vamos uno por uno por todos los servidores
            for server_id in SERVER_IDS:
                
                
                # --- 1. Simulamos el estado del servidor ---
                
                # Uso de CPU (un número al azar entre 5% y 75%)
                cpu_percent = random.uniform(5.0, 75.0)
                if random.random() < 0.1: # Hay un 10% de "mala suerte" de tener un pico brutal de CPU
                    cpu_percent = random.uniform(85.0, 98.0)
                    
                # Uso de Memoria RAM (entre 20% y 85%)
                memory_percent = random.uniform(20.0, 85.0)
                if random.random() < 0.05: # 5% de probabilidad de tener la memoria casi llena
                    memory_percent = random.uniform(90.0, 99.0)
                    
                # Velocidad de disco duro y red a internet
                disk_io_mbps = random.uniform(0.1, 50.0)
                network_mbps = random.uniform(1.0, 100.0)
                
                # Errores (normalmente 0, pero con un 8% de probabilidad de que ocurran entre 1 y 3 errores)
                error_count = 0
                if random.random() < 0.08: 
                    error_count = random.randint(1, 3)
                    
                # --- 2. Empaquetamos los datos ---
                # Creamos un diccionario juntando toda la información que acabamos de inventar
                metric_message = {
                    "server_id": server_id, # El nombre del servidor actual
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(), # Hora exacta universal
                    "metrics": { # Guardamos los valores redondeados a 2 decimales para que queden limpios
                        "cpu_percent": round(cpu_percent, 2),
                        "memory_percent": round(memory_percent, 2),
                        "disk_io_mbps": round(disk_io_mbps, 2),
                        "network_mbps": round(network_mbps, 2),
                        "error_count": error_count
                    },
                    "message_uuid": str(uuid.uuid4()) # Un código de barras único para identificar este mensaje concreto
                }
                
                # --- 3. Enviamos a Kafka ---
                try:
                    # Le entregamos el paquete al productor para que lo envíe al canal (TOPIC)
                    productor.send(KAFKA_TOPIC, value=metric_message)
                    print(f"Enviada métrica de {server_id} a Kafka.")
                    
                except Exception as e:
                     # Si falla el envío de un servidor, avisamos, pero el bucle sigue con el siguiente servidor
                     print(f"Error al enviar métrica de {server_id}: {e}")

            # --- 4. Fin de la ronda ---
            # 'flush' empuja todos los mensajes pendientes de golpe para asegurar que lleguen a Kafka ahora mismo
            productor.flush()
            
            # Ponemos a dormir el programa durante 10 segundos antes de volver a empezar el bucle "while True"
            print(f"\nReporte completo generado y enviado. Esperando {REPORTING_INTERVAL_SECONDS} segundos...")
            time.sleep(REPORTING_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        # Si el usuario pulsa las teclas Ctrl + C en la terminal, el bucle se rompe y entramos aquí
        print("\nSimulación detenida por el usuario.")
    finally:
        # --- Cierre seguro ---
        # Pase lo que pase (se cancele manual o de error), el programa siempre pasará por aquí al terminar
        print("Cerrando el productor de Kafka...")
        # Comprobamos si la variable 'productor' existe (por si acaso falló al principio)
        if 'productor' in locals():
            productor.close() # Nos desconectamos educadamente de Kafka liberando recursos