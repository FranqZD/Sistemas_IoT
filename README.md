# Sistemas_IoT
BDNR PROJECT

# Integrantes 
Luis Francisco Zárate Díaz 751868
Valentina Montaño Sandez 739176
Yana Elina Medina García 750292

# Descripción del proyecto
El proyecto consiste en el desarrollo de un Sistema de Gestión de Dispositivos IoT que integra tres bases de datos NoSQL: MongoDB, Dgraph y Cassandra, aprovechando las fortalezas particulares de cada modelo para resolver distintos requerimientos dentro de un mismo ecosistema.

MongoDB se encargará de la gestión flexible de configuraciones, usuarios y metadatos mediante documentos JSON.

Cassandra almacenará y consultará grandes volúmenes de datos de sensores, logs y alertas con alta disponibilidad y eficiencia temporal.

Dgraph representará la relación entre zonas, instalaciones, clusters, dispositivos, marcas y rutas de comunicación, permitiendo visualizar el grafo completo del sistema IoT.

# Flujo de trabajo
Connect.py
Archivo donde se establece la conexión a las tres bases de datos.

Populate.py
Al correr el populate.py se insertarán los datos en las tres bases de datos. 

Main.py
El main.py se mostrará un menú con opciones para consultar información en cualquiera de las tres bases de datos.

data
Se usará para generar los datos de prueba a través de archivos csv.

Cassandra/, Mongo/, Dgraph/
Se usará para definir la estructura en las tres bases de datos como tablas, esquemas y documentos.