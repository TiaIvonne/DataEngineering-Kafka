# Procesamiento de datos en tiempo real con Kafka

**Autora**: Ivonne Yáñez Mendoza\
**Email**: [ivonne\@imendoza.io](mailto:ivonne@imendoza.io)\
**GitHub**: <https://github.com/TiaIvonne>

![License](https://img.shields.io/badge/license-All%20Rights%20Reserved-red.svg) ![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.8.0-black) ![Status](https://img.shields.io/badge/status-running-green)

## Tabla de Contenidos

1.  [Descripción](#descripción)
2.  [Estructura del Directorio](#estructura-del-directorio)
3.  [Desarrollo del Proyecto](#desarrollo-del-proyecto)
4.  [Soporte](#soporte)
5.  [Licencia](#licencia)

## Descripción {#descripción}

Este proyecto corresponde al módulo de Kafka y Procesamiento de datos en tiempo real del Máster en Ingeniería de datos de la Universidad Complutense de Madrid.

Se debe construir una solución basada en Apache Kafka que permita:

1.  Procesar los datos de sensores agrícolas en tiempo real para detectar condiciones anormales (picos de temperatura & humedad)

2.  Integrar los datos de transacciones de ventas provenientes de una base de datos relacional MySQL utilizando Kafka Connect.

3.  Utilizar un conector que genera datos de prueba (Kafka Connect Datagen) para simular un escenario de procesamiento de datos.

4.  Transformar los datos mediante procesamiento streaming para generar insights del tipo: alertas de anomalías en los sensores y ventas por categoría de producto cada minuto.

## Estructura del directorio {#estructura-del-directorio}

     0.tarea/
      ├── assets/
      ├── connectors/
      ├── datagen/
      ├── sql/
      ├── src/
      ├── pom.xml
      ├── setup.sh
      ├── shutdown.sh
      └── start_connectors.sh

Los directorios más relevantes para esta práctica son:

**connectors**: Contienen los conectores generados en formato JSON los cuales son necesarios para generar datos sintéticos, integrarlos con MySQL, procesar los datos en tiempo real de los sensores e integrar con una base de datos NoSQL.\
**datagen**: Contienen la definición del esquema y del tipo de dato a utilizar en los conectores.\
**sql**: Contiene la definición de la tabla sales_transactions que debe ser procesada en MySQL\
**src**: La fuente del proyecto y donde se encuentra el código base para procesar los datos en tiempo real de FarmIA.

Fuera de la estructura del directorio se encuentran tres scripts que se deben ejecutar en la shell que realizan las siguientes acciones:\
**setup.sh:** Contiene todo lo necesario para levantar Docker y evitar tener que configurar el archivo yml en el directorio.\
**shutdown.sh:** Para detener entorno.\
**start_connectors.sh:** Lanza los conectores en lote en vez de ejecutar cada comando por separado.\
**create-topics.sh:** Crea los topics en lote.\

## Desarrollo del proyecto {#desarrollo-del-proyecto}

### 1. Crear los topics

Se pide la creación de los siguientes topics (contenedor de mensajes)

1.  sensor-telemetry: Contiene datos de los sensores agrícolas.

2.  sales-transactions: Contiene datos de transacciones de ventas.

3.  sensor-alerts: Contiene alertas generadas (picos de temperatura y humedad) generadas al procesar los mensajes.

4.  sales-summary: Contiene el resumen de ventas (con agregaciones) generadas al procesar los datos.

Para crear los topics que es el primer paso de esta práctica es necesario comenzar lanzando el script *setup.sh* que es el encargado de crear el entorno basado en docker el cual construirá el entorno de trabajo para esta práctica:

``` bash
 0.tarea git:(master) ✗ ./setup.sh
  ✔ Container connect  Started 5.6s 
Esperando reinicio contenedor connect
OK
```

![Docker Running](assets/docker-running.png)

Una vez que está corriendo docker es momento de entrar en la consola interactiva y crear los topics:

``` shell-session
0.tarea git:(master) ✗ docker exec -it broker-1 /bin/bash
```

Una vez en la consola se lanzan los siguientes comandos para crear los tópicos:

``` bash
[appuser@broker-1 ~]$ kafka-topics --bootstrap-server broker-1:29092 --create --topic sensor-telemetry --partitions 4 --replication-factor 2 --config max.message.bytes=64000 --config flush.messages=1
```

**Nota personal:** Esto puede ser un poco tedioso por cada topic, por lo que he creado un script de bash que hace esta tarea por cada topic a crear.\
El script se encuentra en el directorio raíz con el nombre de **create-topics.sh**.

``` bash
BOOTSTRAP_SERVER="broker-1:29092"
  PARTITIONS=4
  REPLICATION_FACTOR=2

TOPICS=(
      "sensor-telemetry"
      "sales-transactions"
      "sensor-alerts"
      "sales-summary"
)

for TOPIC in "${TOPICS[@]}"; do
    echo "Creando topic: $TOPIC"
    docker exec broker-1 kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $TOPIC \
        --partitions $PARTITIONS \
        --replication-factor $REPLICATION_FACTOR \
        --config max.message.bytes=64000 \
        --config flush.messages=1
    done
```

Por consola se pueden ver los topics creados:

``` bash
kafka-topics --bootstrap-server broker-1:29092 --list
```

O en el control center:

![Control Center](assets/crear-topics.png)

### 2. Datasets de entrada

Una vez creados los topics, es momento de prestar atención a la estructura o esquema que tendrán los datos a ser procesados. Los datos de transacciones de ventas ya vienen configurados por defecto con este repositorio, se debe crear la estructura de los sensores agrícolas.

Para esto se debe completar el archivo *sensor-telemetry.avsc* con los campos requeridos en la práctica:

-   sensor_id: string

-   timestamp: long pero con una iteración que parte con una fecha razonable para generar datos.

-   temperature: float con un rango entre 15 y 45 grados máximo

-   humidity: float con rango mínimo de 10 y máximo 40

-   soil_fertility: float con rango mínimo de 30 y máximo 100

``` json
{
  "namespace": "com.farmia.iot",
  "name": "SensorTelemetry",
  "type": "record",
  "fields": [
    {
      "name": "sensor_id",
      "type": {
        "type": "string",
        "arg.properties": {
          "regex": "sensor_[0-9]{3}"
        }
      }
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "arg.properties": {
          "iteration": {
            "start": 1741478400000,
            "step": 1000
          }
        }
      }
    },
```

### 3. Tareas a resolver

#### 1. Generación de datos sintéticos con Kafka Connect

Este conector simula las lecturas de los sensores de datos y envía hacia el topic sensor-telemetry.  
Una vez creado el esquema en el punto anterior se pueden configurar los conectores necesarios para procesar transacciones.

En la práctica se incluye un script de bash que lanza todos los conectores de una vez, también se pueden lanzar por separado.

``` json
{
  "name": "source-datagen-sensor-telemetry",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "sensor-telemetry",
    "schema.filename": "/home/appuser/sensor-telemetry.avsc",
    "schema.keyfield": "sensor_id",
    "max.interval": 500,
    "iterations": 10000000,
    "tasks.max": "1"
  }
}
```

Con este comando se registra el conector:

``` bash
  curl -d @"./connectors/source-datagen-sensor-telemetry.json" \
    -H "Content-Type: application/json" \
    -X POST http://localhost:8083/connectors | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   717  100   373  100   344   3693   3406 --:--:-- --:--:-- --:--:--  7170
{
  "name": "source-datagen-sensor-telemetry",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "sensor-telemetry",
    "schema.filename": "/home/appuser/sensor-telemetry.avsc",
    "schema.keyfield": "sensor_id",
    "max.interval": "500",
    "iterations": "10000000",
    "tasks.max": "1",
    "name": "source-datagen-sensor-telemetry"
  },
  "tasks": [],
  "type": "source"
}
```

Se puede comprobar en el control center la creación de los mensajes y también en la shell utilizando este comando:

``` bash
  docker exec -it schema-registry kafka-avro-console-consumer \
    --bootstrap-server broker-1:29092 \
    --topic sensor-telemetry
```

![Sensor telemetry en tiempo real](assets/sensor-telemetry1.gif)

#### 2. Integración de MySQL con Kafka Connect

Se debe configurar un conector que lea datos desde una base de datos en MySQL que contiene las transacciones de ventas.

Esta tabla es la tabla transactions que ya está configurada con antelación en el proyecto.

Este topic es algo distinto al anterior pues para funcionar correctamente se deben lanzar dos conectores internos que generan datos en datagen source-datagen-\_transactions y sink-mysql-\_transactions.

Una vez lanzados se puede observar los datos que están guardándose en sales_transactions en MySQL. En este caso se ha utilizado DBeaver para mostrar los resultados:

![MySQL](assets/mysql-sales.png)

Con estas preparaciones previas y configurado el conector **source-mysql-sales_transactions.json** se puede lanzar el comando:

``` bash
curl -d @"./connectors/source-mysql-sales_transactions.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  1611  100   798  100   813   2426   2471 --:--:-- --:--:-- --:--:--  4911
{
  "name": "source-mysql-sales_transactions",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://mysql:3306/db?useSSL=false&allowPublicKeyRetrieval=true",
    "connection.user": "user",
    "connection.password": "password",
    "table.whitelist": "db.sales_transactions",
    "mode": "timestamp",
    "timestamp.column.name": "timestamp",
    "poll.interval.ms": "1000",
    "tasks.max": "1",
    "transforms": "renametopic,setkey",
    "transforms.renametopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.renametopic.regex": ".*",
    "transforms.renametopic.replacement": "sales-transactions",
    "transforms.setkey.type": "org.apache.kafka.connect.transforms.ValueToKey", 
    "transforms.setkey.fields": "transaction_id",
    "name": "source-mysql-sales_transactions"
  },
  "tasks": [],
  "type": "source"
}
```

Se pueden ver los resultados en el control center o directamente en la terminal utilizando el comando:

``` bash
 docker exec -it schema-registry kafka-avro-console-consumer \
    --bootstrap-server broker-1:29092 \
    --topic sales-transactions 2>/dev/null
```

![Sales Transactions](assets/sales-transactions1.gif)

En el control center se puede revisar un mensaje con más detalle:

``` json
{
  "transaction_id": "tx52977",
  "product_id": "prod_536",
  "category": "equipment",
  "quantity": 4,
  "price": "\u0011r",
  "timestamp": 1773430169000
}
```

#### 3. Procesamiento en tiempo real de sensores

En este apartado el objetivo es escribir una aplicación que procese los datos del topic sensor-telemetry.

Cuando se detecten condiciones anómalas de temperatura mayores a 35 grados o humedad menor al 20% debe enviar esas alertas a **sensor-alerts** con un formato tipo de mensaje:

``` json
{
  "sensor_id": "sensor_001",
  "alert_type": "HIGH_TEMPERATURE",
  "timestamp": 1741479493000,
  "details": "Temperature exceeded 35C"
}
```

Para esto se ha creado la aplicación **SensorAlerterApp.java**.  

La App se encuentra aquí: *src/main/java/com/farmia/streaming/SensorAlerterApp.java*

Algunos puntos de interés:

En la app se indica el topic de entrada y salida:

``` java
private static final String INPUT_TOPIC = "sensor-telemetry";
private static final String OUTPUT_TOPIC = "sensor-alerts";
private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
```

El filtro solicitado:

``` java
.filter((key, record) -> {
    float temperature = (float) record.get("temperature");
    float humidity = (float) record.get("humidity");
    return temperature > 35.0f || humidity < 20.0f;
})
```

Con este comando **mvn exec:java -Dexec.mainClass="com.farmia.streaming.SensorAlerterApp"** se ejecuta la App.

En la terminal se puede ver la generación de mensajes con picos de temperatura y/o humedad

![Sensor Alerts](assets/sensor-alert.gif)

Y en el control center un detalle del mensaje generado el cual es acorde a lo solicitado en las instrucciones:

``` json
{
  "sensor_id": "sensor_717",
  "alert_type": "HIGH_TEMPERATURE",
  "timestamp": 1741479493000,
  "details": "Temperature exceeded 35C: 35.15159"
}
{
  "sensor_id": "sensor_127",
  "alert_type": "LOW_HUMIDITY",
  "timestamp": 1741479939000,
  "details": "Humidity below 20%: 17.266754"
}
```

#### 4. Procesamiento en tiempo real de transacciones de ventas

Tal como en el caso anterior, el objetivo aquí es generar una App que procese **sales-transactions** donde sea capaz de agrupar los datos por categoría de producto y calcule los ingresos por categoría cada minuto.

El topic que recibirá estos mensajes es **sales-summary** con un tipo de mensaje específico:

``` json
{
  "category": "fertilizers",
  "total_quantity": 20,
  "total_revenue": 1000.10,
  "window_start": 1773515220000,
  "window_end": 1773515280000
}
```

La App se encuentra aquí: *src/main/java/com/farmia/streaming/SalesSummaryApp.java*.

Dentro del archivo se encuentran las configuraciones para poder generar las agregaciones:

``` java
private static final String INPUT_TOPIC = "sales-transactions";
private static final String OUTPUT_TOPIC = "sales-summary";
private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
```

En este apartado del código (líneas 37 a 44) se configura la agrupación por categoría y el procesamiento en intervalos de un minuto.

``` java
// Configuracion de los requerimientos de agrupacion de los datos de categoria x minuto
    .groupBy(
        (key, record) ->record.get("category").toString(),
        Grouped.with(Serdes.String(), genericAvroSerde))
// Configura el procesamiento en intervalos de 1 minuto
    .windowedBy(
        TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
)
```

Y aquí es donde se configura el criterio de agregación para calcular el total de ingresos:

``` java
// Criterios de agregacion para obtener el totalRevenue
    .aggregate(
// Cuando llega la primera transaccion este es el valor de partida
    () -> "0,0.0",
    (category, record, accumulator) -> {
    String[] parts = accumulator.split(",");
    int totalQuantity = Integer.parseInt(parts[0]) + (int) record.get("quantity");
// Este bloque convierte el decimal de MySQL a formato double
    Conversions.DecimalConversion decimalConversion = new org.apache.avro.Conversions.DecimalConversion();
    org.apache.avro.Schema priceSchema = record.getSchema().getField("price").schema();
    double price = decimalConversion.fromBytes(
    (java.nio.ByteBuffer) record.get("price"),
    priceSchema,
    priceSchema.getLogicalType()
).doubleValue();
```

Dado que en MySQL la columna price viene con formato decimal, se ha utilizado un conversor de avro para convertirlo a doubleValue y desplegar el formato correcto en el mensaje saliente (ver líneas 53 al 59)

Es momento de ejecutar la aplicación con el comando **mvn exec:java -Dexec.mainClass="com.farmia.streaming.SalesSummaryApp"**  

En la terminal se ve la generación de mensajes:

![Sales Summary](assets/sales-summary.gif)

En el control center se puede ver un mensaje en formato json:

``` json
{
  "category": "seeds",
  "total_quantity": 22,
  "total_revenue": 3260.85,
  "window_start": 1773514260000,
  "window_end": 1773514320000
}
```

Durante la creación de las App al ejecutar por primera vez con el comando mvn lanzaba error de compilación y cerraba sin procesar mensajes.  
Para resolver esto en ambas App se añadió al bloque main un InterruptedException y un Thread.currentThread().join() con el join se evita que se cierre sin enviar mensajes:

``` java
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        Topology topology = createTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        // esto evita que los hilos terminen a los segundos sin enviar mensajes
        Thread.currentThread().join();
    }
}
```

#### 5. Integración de MongoDB con Kafka Connect

El objetivo de este último ejercicio es escribir los datos de sensor-alerts a una colección en MongoDB utilizando un sink connector.

Para esto se ha creado el conector **sink-mongodb-sensor_alerts.json**

``` json
{
  "name": "sink-mongodb-sensor_alerts",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "topics": "sensor-alerts",
    "connection.uri": "mongodb://admin:secret123@mongodb:27017",
    "database": "farmia",
    "collection": "sensor_alerts",
    "tasks.max": "1",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy"
  }
}
```

A modo de repaso, estos mensajes de alerta se generan desde la App SensorAlerter que ya ha enviado mensajes al control center de tipo:

``` json
{
  "sensor_id": "sensor_716",
  "alert_type": "HIGH_TEMPERATURE",
  "timestamp": 1741482888000,
  "details": "Temperature exceeded 35C: 38.30977"
}
```

Con el sink connector se envían estos mensajes generados en sensor-alerts a MongoDB. En la terminal se puede revisar cómo se están generando los mensajes que están llegando a MongoDB.

Utilizando bash es posible generar los mensajes en formato json:

``` bash
while true; do
    docker exec mongodb mongosh -u admin -p secret123 --quiet \
      --eval 'db.getSiblingDB("farmia").sensor_alerts.find().sort({$natural:-1}).limit(5).forEach(doc => print(EJSON.stringify(doc, null, 2)))'
    sleep 0.5
  done
```

![Mongo](assets/mongo-terminal.png)

También se puede revisar en MongoDB Compass:

![Mongo Compass](assets/mongo-compass.png)

Con esto se cubre la práctica completa donde se ha explorado el ciclo de obtener datos, agruparlos, crear nuevos mensajes a partir de los datos entrantes e interactuar con bases de datos relacionales y NoSQL.

Al momento de terminar es momento de detener el container utilizando ./shutdown.sh:

``` bash
Deteniendo entorno
[+] Running 14/13
 ✔ Container mongodb             Removed                                                                                                                                                1.7s 
 ✔ Container control-center      Removed                                                                                                                                                7.8s 
 ✔ Container ksqldb-cli          Removed                                                                                                                                                0.8s 
 ✔ Container mysql               Removed                                                                                                                                                7.7s 
 ✔ Container ksqldb-server       Removed                                                                                                                                                3.3s 
 ✔ Container connect             Removed                                                                                                                                               13.2s 
 ✔ Container schema-registry     Removed                                                                                                                                                2.6s 
 ✔ Container broker-2            Removed                                                                                                                                                5.4s 
 ✔ Container broker-1            Removed                                                                                                                                                5.3s 
 ✔ Container broker-3            Removed                                                                                                                                                5.3s 
 ✔ Container controller-3        Removed                                                                                                                                                7.2s 
 ✔ Container controller-1        Removed                                                                                                                                                1.7s 
 ✔ Container controller-2        Removed                                                                                                                                                1.7s 
 ✔ Network 1environment_default  Removed                                                                                                                                                0.1s 
OK
```

## Licencia {#licencia}

Todos los derechos reservados
