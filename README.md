# trabajo_clase_2
1. ¿Qué es una Base de Datos Vectorial?
Una base de datos vectorial es un tipo de base de datos optimizada para almacenar, indexar y buscar datos en forma de vectores de alta dimensión. Se utilizan comúnmente en aplicaciones de inteligencia artificial, aprendizaje automático, recuperación de información y búsqueda semántica.
En términos simples, una base de datos vectorial permite encontrar elementos similares entre sí basándose en métricas de distancia (como distancia euclidiana, coseno o Manhattan), lo que la hace ideal para tareas como:
•   Búsqueda de imágenes o videos similares
•   Sistemas de recomendación
•   Procesamiento de lenguaje natural (NLP)
•   Reconocimiento facial
•   Detección de anomalías
 
¿Cómo implementar bases de datos vectoriales en PostgreSQL?
PostgreSQL no es una base de datos vectorial nativa, pero se puede extender con módulos como pgvector para manejar búsquedas vectoriales de manera eficiente.
Instalar la extensión pgvector
Para habilitar el soporte de bases de datos vectoriales en PostgreSQL, primero instalar la extensión pgvector.
En una base de datos PostgreSQL existente
CREATE EXTENSION vector;
 
Crear una tabla con una columna vectorial
Una vez instalada la extensión, puedes crear una tabla con una columna de tipo vector. Por ejemplo, para almacenar vectores de dimensión 3:
CREATE TABLE items (
    id SERIAL PRIMARY KEY,
    embedding vector(3)
);

Insertar datos vectoriales
INSERT INTO items (embedding) VALUES ('[1.0, 2.0, 3.0]');
INSERT INTO items (embedding) VALUES ('[2.5, 3.1, 4.0]');
INSERT INTO items (embedding) VALUES ('[0.5, 0.2, 0.1]');

Buscar el vector más cercano
Para encontrar el vector más cercano a [1.5, 2.5, 3.5] usando la distancia euclidiana:

SELECT * FROM items ORDER BY embedding <-> '[1.5, 2.5, 3.5]' LIMIT 1;
 
Aquí <-> representa la métrica de distancia euclidiana. También puedes usar #> para distancia de coseno o <#> para distancia Manhattan.

Optimizar búsquedas con índices
Para mejorar la velocidad de búsqueda, puedes crear un índice ivfflat:
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops)
WITH (lists = 100);
 
Donde:
•   vector_l2_ops usa la distancia euclidiana.
•   vector_ip_ops usa el producto interno.
•   vector_cosine_ops usa la similitud de coseno.
 
Las bases de datos vectoriales en PostgreSQL, como las que se pueden implementar con la extensión pgvector, tienen una amplia gama de aplicaciones en diversos campos, como:
 
Procesamiento del Lenguaje Natural (PLN): Las bases de datos vectoriales son ideales para almacenar y buscar representaciones vectoriales de texto, conocidas como embeddings. Esto permite realizar tareas como la búsqueda semántica, la clasificación de texto y la traducción automática.
 
Visión por Computador (VC): En aplicaciones de visión por computador, las bases de datos vectoriales pueden almacenar y buscar representaciones vectoriales de imágenes. Esto es útil para tareas como el reconocimiento de objetos, la búsqueda de imágenes similares y la detección de anomalías.
 
Sistemas de Recomendación (SR): Los sistemas de recomendación pueden beneficiarse de las bases de datos vectoriales al almacenar y buscar representaciones vectoriales de usuarios y productos. Esto permite realizar recomendaciones personalizadas basadas en la similitud de los vectores.
 
Aprendizaje Automático: Las bases de datos vectoriales son esenciales para almacenar y buscar grandes conjuntos de datos de alta dimensión utilizados en modelos de aprendizaje automático. Esto incluye tareas como la clasificación, la regresión y el clustering.
 
Búsqueda de Similitud: Las bases de datos vectoriales permiten realizar búsquedas de similitud utilizando métricas como la distancia euclidiana, la distancia coseno y la distancia Manhattan. Esto es útil en aplicaciones donde se necesita encontrar elementos similares a un vector de consulta.
 
Integración con Herramientas de IA: La extensión pgvector permite integrar PostgreSQL con herramientas de inteligencia artificial, mejorando las capacidades de búsqueda y almacenamiento de datos vectoriales en una base de datos relacional.

2. ¿Qué es y que aplicaciones tienen los Datalakes?
Data Lake es un repositorio de almacenamiento que contiene una gran cantidad de datos en su formato original, utiliza una arquitectura plana para almacenar datos de diferentes fuentes y permite trabajar con ellos sin preocuparse por la transformación previa de los datos en un formato específico. Funciona como un gran lago lleno de información que permite almacenar, compartir, explorar, navegar y gobernar para descubrir patrones, predecir comportamientos y comprender los datos. 
El almacenamiento de datos se da de forma original sin estructurarlos ni limpiarlos previamente para que se pueda almacenar y procesar datos sin esquema y en cualquier formato sin la necesidad de conocer cómo se van a explotar en el futuro. Esta característica evita que sean necesarios complejos procesos ETL (Extracción, Transformación y Carga) de limpieza y preparación.

Los Datalakes pueden ser empresariales, personales, en la nube, on-premise, etc

Un data lake es capaz de proporcionar datos a la organización para una gran variedad de procesos analíticos:
•	Descubrimiento y exploración de datos
•	Análisis ad hoc simple
•	Análisis complejo para toma de decisiones
•	Informes
•	Análisis en tiempo real

Un Data Lake sirve para muchas tareas, especialmente en el área empresarial, como:
Almacenamiento a escala de grandes volúmenes de datos: El principal beneficio de un data lake es la centralización de fuentes de contenido dispares. Una vez reunidas, estas fuentes pueden ser combinadas y procesadas utilizando big data, búsquedas y análisis que de otro modo hubiera sido imposible. Al ser tan escalables, manejan más datos que otros sistemas tradicionales de almacenamiento. 

Análisis Ad Hoc y avanzado de datos: Permiten la capacidad de analizar grandes volúmenes de datos, facilitando la exploración y el análisis de forma flexible y avanzada. Permiten procesar análisis avanzados, como el aprendizaje automático y la inteligencia artificial. Las técnicas de aprendizaje automático y la IA dan paso a las empresas a descubrir patrones y tendencias en sus conjuntos de datos que serían difíciles de detectar por otros medios.

Análisis en Tiempo Real: Los Data Lakes pueden integrarse con herramientas de análisis en tiempo real para proporcionar información inmediata y relevante para la toma de decisiones rápidas. Un Data Lake Inteligente permite preparar y compartir rápidamente datos que son fundamentales para ofrecer analíticas competitivas

Investigaciones de ciencia de datos: Los Data lakes también proporcionan un entorno ideal para la investigación de ciencia de datos. Permite a los equipos acceder a un banco de datos escalable, y al mismo tiempo facilitan el intercambio de información y la colaboración entre ellos. Por lo tanto, la investigación de ciencia de datos en un entorno más fácil de manejar.

Centralización de Datos y Reducción de Costos: Los Data Lakes permiten centralizar datos de diferentes fuentes, eliminando los silos de información y facilitando el acceso y procesamiento de datos de manera más eficiente. Al centralizar el almacenamiento de datos y permitir el acceso a datos en bruto, reduce los costos asociados con la transformación y almacenamiento de datos en múltiples sistemas.

La seguridad es una preocupación importante para empresas que trabajan con grandes cantidades de datos, es posible construir un data lake administrado y gobernado para proteger los datos, considerando controles de acceso a lo largo del ciclo de vida de los datos, encriptación y enmascaramiento de datos, auditoría y monitoreo así como establecer procedimientos y políticas claras de gobernanza de datos. Es muy importante que el datalake proporcione suficiente trazabilidad, y de esta manera poder determinar los cambios que han sufrido los datos en los procesos de transformación o ingesta.
