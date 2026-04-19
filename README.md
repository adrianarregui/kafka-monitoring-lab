## 📊 Monitoreo de Infraestructura con Kafka y MongoDB
# 
## ¡Hola! En este repositorio comparto mi práctica para el curso de **Big Data Aplicado**. 
## El objetivo principal fue montar un pipeline de datos completo: desde la generación 
## de métricas de servidores en tiempo real hasta su almacenamiento y análisis en la nube.
# 
## La idea era simular un entorno real donde varios servidores reportan su estado 
## (CPU, RAM, Disco, etc.) y nosotros debemos capturar esa información, guardarla 
## y sacar estadísticas rápidas (KPIs).
# 
# # ## 🛠️ Tecnologías utilizadas
# # * **Python**: Para programar tanto el productor como el consumidor.
# # * **Apache Kafka**: Como motor de mensajería (corriendo en Docker).
# # * **MongoDB Atlas**: Base de datos NoSQL en la nube para guardar los resultados.
# # * **Docker**: Para levantar la infraestructura local de Kafka de forma rápida.
# 
# # ---
# 
# # ## 🏗️ ¿En qué consistía la práctica?
# 
# # El proyecto se divide en dos piezas clave que trabajan juntas:
# 
# # ### 1. El Productor (`productor_metrics_iabd09.py`)
# # Es un script que simula 5 servidores (`web01`, `db01`, etc.). Cada 10 segundos, 
# # genera métricas aleatorias pero realistas y las envía a un "topic" de Kafka. 
# # Es como el sensor que envía la información a la central.
# 
# # ### 2. El Consumidor (`consumidor_metrics_iabd09.py`)
# # Esta es la parte inteligente. El consumidor escucha a Kafka y, por cada mensaje 
# # que recibe, hace dos cosas:
# # 1. **Guarda la métrica bruta**: La inserta en una colección de MongoDB Atlas.
# # 2. **Calcula KPIs**: Cada vez que junta **20 mensajes**, calcula promedios de CPU, 
# # memoria y red, y mide la velocidad de procesamiento.
# 
# # ---
# 
# # ## 🚀 Pasos resumidos para ponerlo en marcha
# 
# # 1. **Levantar Kafka**: Usar el archivo `docker-compose.yml` para encender el broker local.
# # 2. **Configurar MongoDB Atlas**: Crear el cluster y obtener la URI de conexión.
# # 3. **Lanzar el Productor**: Ejecutar el script para empezar a enviar datos.
# # 4. **Lanzar el Consumidor**: Ejecutar el segundo script para procesar y subir los datos.
# # 5. **Verificar**: Entrar en MongoDB Atlas y ver las colecciones llenándose en tiempo real.
# 
# # ---
# 
# # ## 📈 Resultado Final
# # Logré un sistema que procesa datos en tiempo real y genera resúmenes automáticos 
# # cada 20 mensajes, dejando la base lista para crear un dashboard de visualización.
# 
# # ---
