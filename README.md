# Migración de Procesos Batch — Banco XYZ

Proyecto de las Semanas 1, 2 y 3 de **Desarrollo Backend III (PBY2203)**.

## 1. Objetivo del proyecto

Modernizar tres procesos batch del sistema legacy del banco, garantizando la integridad y
consistencia de los datos mediante validación, transformación, manejo de errores,
tolerancia a fallos y procesamiento paralelo:

1. **Reporte de Transacciones Diarias** — detecta anomalías y genera un resumen diario.
2. **Cálculo de Intereses Mensuales** — aplica intereses sobre cuentas de ahorro/préstamo/hipoteca
   y actualiza el saldo final.
3. **Generación de Estados de Cuenta Anuales** — compila el historial anual de cada cuenta para
   auditorías.

Sobre la base de la Semana 1 (Job / Step / Reader / Processor / Writer) y la Semana 2
(multithreading simple, tolerancia a fallos), la Semana 3 agrega:

- **Particionamiento real** con `Partitioner` + `PartitionHandler`: cada Job tiene un Step
  maestro que reparte el trabajo en 3 particiones, cada una ejecutada en su propio hilo,
  reemplazando el multithreading a nivel de chunk de la semana anterior.
- **Retry declarativo con backoff exponencial**: en vez de una `RetryPolicy` personalizada,
  se usa la configuración fluida de Spring Batch (`.retry(...).retryLimit(...).backOffPolicy(...)`).
- **SkipPolicy de "lista blanca"**: solo se toleran los tipos de excepción explícitamente
  reconocidos como errores de datos esperables; cualquier excepción no prevista detiene el
  Job en vez de saltarse en silencio.
- **`RunIdIncrementer`** en los 3 Jobs, para que cada ejecución (por consola o por API) sea
  siempre tratada como una corrida nueva, sin importar los parámetros.
- **Dataset ampliado** con más registros y más variantes de datos mal clasificados
  (formatos de fecha adicionales, tipos de transacción inválidos, tildes inconsistentes,
  montos vacíos o con signo incorrecto).

## 2. Estructura del código

```
src/main/java/cl/duocuc/bancoxyz/
 ├─ BatchMigracionBancoXyzApplication.java   Clase principal (Spring Boot)
 ├─ config/
 │   ├─ JobTransaccionesDiariasConfig.java    Job 1: Step maestro + particiones
 │   ├─ JobInteresesMensualesConfig.java      Job 2: Step maestro + particiones
 │   ├─ JobEstadosCuentaAnualesConfig.java    Job 3: Step maestro + particiones
 │   └─ PartitionTaskExecutorConfig.java       Pool de hilos usado por el PartitionHandler
 ├─ controller/
 │   └─ BatchController.java                   Endpoints REST para disparar cada Job (Postman)
 ├─ exception/
 │   ├─ DatoInvalidoException.java              Error de validación de negocio (se salta)
 │   └─ ConexionTransitoriaException.java       Error transitorio de infraestructura
 ├─ model/          Entidades JPA (datos válidos, anomalías y resúmenes) + DTOs de CSV
 ├─ partitioners/
 │   └─ SimpleGridPartitioner.java              Reparte el trabajo en N particiones (grid)
 ├─ policy/
 │   ├─ ChunkCompletionPolicy.java              Cierra el chunk por tamaño o por tiempo
 │   └─ GenericSkipPolicy.java                  Lista blanca de excepciones tolerables
 ├─ processor/       ItemProcessor de cada Job: valida, corrige, detecta anomalías y filtra
 │                    por partición (cada hilo procesa solo el subconjunto que le corresponde)
 ├─ repository/       Interfaces Spring Data JPA
 └─ listener/
     ├─ GenericSkipListener.java              Captura errores técnicos de lectura/proceso/escritura
     ├─ GenericStepListener.java              Métricas de inicio/fin de cada Step (por partición)
     └─ BatchJobCompletionListener.java        Resumen agregado de todas las particiones al finalizar

src/main/resources/
 ├─ application.yml            Configuración de MySQL, Spring Batch y pool de particiones
 ├─ application-secrets.yml    Contraseña de BD — NO se sube al repositorio (ver sección 4)
 └─ data/                       CSV de entrada (transacciones.csv, intereses.csv, cuentas_anuales.csv)
```

### Manejo de errores y calidad de datos

Cada `ItemProcessor` aplica las reglas de negocio del proceso y, si una fila no cumple, la
registra en una tabla `*_anomalias` con el motivo exacto del rechazo y lanza
`DatoInvalidoException` (no detiene el Job). Esta excepción es evaluada por
`GenericSkipPolicy`, que la tolera hasta el límite configurado (`app.batch.skip-limit`), y
`GenericSkipListener` deja constancia en el log de cada registro saltado.

`GenericSkipPolicy` sigue una estrategia de **lista blanca**: solo tolera
`DatoInvalidoException` (errores de negocio) y `FlatFileParseException` (línea del CSV
imposible de mapear). Cualquier otra excepción no prevista **detiene el Job de inmediato**,
en vez de saltarse en silencio — así ningún error inesperado queda oculto.

| Job | Reglas aplicadas |
|---|---|
| Transacciones diarias | monto > 0, fecha válida (`yyyy-MM-dd` o `yyyy/MM/dd`), tipo débito/crédito, sin duplicados |
| Intereses mensuales | saldo ≥ 0, edad 18–100, tipo ahorro/préstamo/hipoteca, sin `cuenta_id` duplicado |
| Estados de cuenta anuales | fecha válida (`yyyy-MM-dd`, `yyyy/MM/dd`, `dd-MM-yyyy` o `dd/MM/yyyy`), descripción no vacía, tipo depósito/retiro/compra, signo del monto coherente con el tipo, sin duplicados |

### Particionamiento y tolerancia a fallos

Cada uno de los 3 Jobs sigue la misma estructura Step maestro → 3 particiones:

- **`SimpleGridPartitioner`**: genera 3 particiones (una por hilo), cada una identificada
  con un `partitionIndex` (0, 1 o 2) y el `gridSize` total, guardados en el
  `ExecutionContext` de cada partición.
- **Filtro en el `ItemProcessor`**: cada partición lee el CSV completo, pero solo procesa
  los registros cuyo identificador (`id` o `cuenta_id`) cumple
  `id % gridSize == partitionIndex`. Así, los 3 hilos procesan subconjuntos disjuntos sin
  pisarse ni duplicar trabajo.
- **`PartitionTaskExecutorConfig`**: define el pool de 3 hilos (`ThreadPoolTaskExecutor`)
  que ejecuta las particiones en paralelo. Los tamaños del pool se externalizan en
  `application.yml` (`bancoxyz.particion.*`).
- **Retry declarativo**: `.retry(SQLTransientException.class).retryLimit(3).backOffPolicy(new ExponentialBackOffPolicy())`
  reintenta automáticamente fallos transitorios de conexión a la base de datos, esperando
  progresivamente más tiempo entre cada intento.

Spring Batch **agrega automáticamente** los contadores (leídos/escritos/saltados) de las 3
particiones dentro del `StepExecution` del Step maestro. `BatchJobCompletionListener` tiene
esto en cuenta al calcular el total del Job, excluyendo los Steps maestros (identificados
por el prefijo `masterStep`) para no contar esos datos dos veces.

## 3. Prerrequisitos

- JDK 17+
- Maven 3.9+
- MySQL 8 corriendo localmente

## 4. Configuración de la base de datos

```sql
CREATE DATABASE banco_xyz_batch CHARACTER SET utf8mb4;
```

La contraseña de la base de datos no está incluida en el repositorio por seguridad.
Antes de ejecutar el proyecto, crea el archivo `src/main/resources/application-secrets.yml`
(excluido en `.gitignore`) con:

```yaml
spring:
  datasource:
    password: CONTRASEÑA_MYSQL
```

El resto de la configuración (URL, usuario, driver, tamaño del pool de particiones) ya está
en `application.yml` y no requiere cambios si tu usuario de MySQL es `root`.

Al levantar la aplicación, Hibernate (`ddl-auto: update`) crea automáticamente todas las
tablas de negocio, y Spring Batch crea sus propias tablas de metadata
(`BATCH_JOB_INSTANCE`, `BATCH_STEP_EXECUTION`, etc.). La base de datos misma se crea sola
si no existe (`createDatabaseIfNotExist=true`).

## 5. Cómo ejecutar el proyecto

Compilar:
```bash
mvn clean package -DskipTests
```

### Opción A — Por consola (línea de comandos)

Cada Job se dispara de forma independiente indicando su nombre con la propiedad
`spring.batch.job.name`:

```bash
# Job 1: Reporte de Transacciones Diarias
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=reporteTransaccionesDiariasJob --spring.batch.job.enabled=true"

# Job 2: Cálculo de Intereses Mensuales
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=calculoInteresesMensualesJob --spring.batch.job.enabled=true"

# Job 3: Generación de Estados de Cuenta Anuales
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=generacionEstadosCuentaAnualesJob --spring.batch.job.enabled=true"
```

Gracias al `RunIdIncrementer` configurado en cada Job, se puede correr el mismo comando
varias veces seguidas sin necesidad de parámetros adicionales — cada corrida se trata como
una ejecución nueva.

Cada corrida imprime en consola, por cada partición, las métricas de `GenericStepListener`
(leídos/escritos/saltados), y al final un resumen agregado de `BatchJobCompletionListener`
con el total del Job — esa consola es evidencia de ejecución. Al finalizar, la aplicación
queda corriendo como servidor web (Tomcat); se cierra con `Ctrl+C`.

### Opción B — Vía API REST (Postman)

Ejecuta la aplicación normalmente (botón Run sobre `BatchMigracionBancoXyzApplication`),
y con la app corriendo en el puerto 8080, dispara cada Job con una petición `POST` sin
body:

| Job | Endpoint |
|---|---|
| Transacciones diarias | `POST http://localhost:8080/api/batch/transacciones-diarias` |
| Intereses mensuales | `POST http://localhost:8080/api/batch/intereses-mensuales` |
| Estados de cuenta anuales | `POST http://localhost:8080/api/batch/estados-cuenta-anuales` |

## 6. Tablas generadas

| Job | Tabla de datos válidos | Tabla de anomalías | Tabla de resultado/resumen |
|---|---|---|---|
| 1 | `transacciones` | `transacciones_anomalias` | `resumen_transacciones_diarias` |
| 2 | `cuentas_intereses` (incluye saldo final) | `cuentas_intereses_anomalias` | — |
| 3 | `movimientos_anuales` | `movimientos_anuales_anomalias` | `estados_cuenta_anuales` |