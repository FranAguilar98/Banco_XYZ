# Migración de Procesos Batch — Banco XYZ

Proyecto de la Semana 1 y 2 de **Desarrollo Backend III (PBY2203)**.

## 1. Objetivo del proyecto

Modernizar tres procesos batch del sistema legacy del banco, garantizando la integridad y
consistencia de los datos mediante validación, transformación, manejo de errores,
tolerancia a fallos y procesamiento paralelo:

1. **Reporte de Transacciones Diarias** — detecta anomalías y genera un resumen diario.
2. **Cálculo de Intereses Mensuales** — aplica intereses sobre cuentas de ahorro/préstamo/hipoteca
   y actualiza el saldo final.
3. **Generación de Estados de Cuenta Anuales** — compila el historial anual de cada cuenta para
   auditorías.

Sobre la base de la Semana 1 (Job / Step / Reader / Processor / Writer), la Semana 2 agrega:

- **Procesamiento multihilo** mediante un `TaskExecutor` (3 hilos en paralelo, chunks de
  tamaño 5).
- **Tolerancia a fallos** con `faultTolerant()`: `SkipPolicy` + `RetryPolicy`
  personalizadas.
- **CompletionPolicy personalizada**: cierra el chunk por cantidad o por tiempo transcurrido.
- **Excepciones personalizadas** para distinguir errores de datos (se saltan) de errores
  de infraestructura (se reintentan o detienen el Job).
- **API REST** para disparar cada Job manualmente (además de la ejecución por consola).

## 2. Estructura del código

```
src/main/java/cl/duocuc/bancoxyz/
 ├─ BatchMigracionBancoXyzApplication.java   Clase principal (Spring Boot)
 ├─ config/
 │   ├─ JobTransaccionesDiariasConfig.java    Job 1: reader/processor/writer/steps
 │   ├─ JobInteresesMensualesConfig.java      Job 2: reader/processor/writer/steps
 │   ├─ JobEstadosCuentaAnualesConfig.java    Job 3: reader/processor/writer/steps
 │   └─ TaskExecutorConfig.java                Pool de 3 hilos para procesamiento paralelo
 ├─ controller/
 │   └─ BatchController.java                   Endpoints REST para disparar cada Job (Postman)
 ├─ exception/
 │   ├─ DatoInvalidoException.java              Error de validación de negocio (se salta)
 │   └─ ConexionTransitoriaException.java       Error transitorio de infraestructura (se reintenta)
 ├─ model/          Entidades JPA (datos válidos, anomalías y resúmenes) + DTOs de CSV
 ├─ policy/
 │   ├─ ChunkCompletionPolicy.java              Cierra el chunk por tamaño o por tiempo
 │   ├─ GenericSkipPolicy.java                  Decide qué excepciones se toleran (saltan)
 │   └─ GenericRetryPolicy.java                 Reintenta solo fallos transitorios de BD
 ├─ processor/       ItemProcessor de cada Job: valida, corrige y detecta anomalías
 ├─ repository/       Interfaces Spring Data JPA
 └─ listener/
     ├─ GenericSkipListener.java              Captura errores técnicos de lectura/proceso/escritura
     ├─ GenericStepListener.java              Métricas de inicio/fin de cada Step
     └─ BatchJobCompletionListener.java        Imprime resumen de cada corrida (leídos/escritos/saltados)

src/main/resources/
 ├─ application.yml            Configuración de MySQL y Spring Batch (sin credenciales)
 ├─ application-secrets.yml    Contraseña de BD — NO se sube al repositorio (ver sección 4)
 └─ data/                       CSV de entrada (transacciones.csv, intereses.csv, cuentas_anuales.csv)
```

### Manejo de errores y calidad de datos

Cada `ItemProcessor` aplica las reglas de negocio del proceso y, si una fila no cumple, la
registra en una tabla `*_anomalias` con el motivo exacto del rechazo y lanza
`DatoInvalidoException` (no detiene el Job). Esta excepción es evaluada por
`GenericSkipPolicy`, que la tolera hasta el límite configurado (`app.batch.skip-limit`), y
`GenericSkipListener` deja constancia en el log de cada registro saltado.

Los errores de conexión a base de datos (`SQLException`) nunca se saltan — detienen el
Job, ya que continuar no tiene sentido si la base de datos no está disponible.
`GenericRetryPolicy` reintenta automáticamente fallos transitorios de conexión antes de que
se propaguen como error definitivo.

| Job | Reglas aplicadas |
|---|---|
| Transacciones diarias | monto > 0, fecha válida (`yyyy-MM-dd` o `yyyy/MM/dd`), tipo débito/crédito, sin duplicados |
| Intereses mensuales | saldo ≥ 0, edad 18–100, tipo ahorro/préstamo/hipoteca, sin `cuenta_id` duplicado |
| Estados de cuenta anuales | fecha válida, descripción no vacía, signo del monto coherente con el tipo (depósito > 0, retiro/compra < 0), sin duplicados |

### Tolerancia a fallos y procesamiento paralelo

Cada Step de carga (`stepCargaTransacciones`, `stepCalculoIntereses`,
`stepCargaMovimientosAnuales`) está configurado con:

- **`ChunkCompletionPolicy`**: cierra el chunk al llegar a 5 items, o a los 2 segundos,
  lo que ocurra primero.
- **`taskExecutor`**: 3 hilos ejecutando el Step en paralelo (`ThreadPoolTaskExecutor`).
- **`skipPolicy` / `retryPolicy`** personalizadas en vez de la configuración por defecto
  de Spring Batch.

El `FlatFileItemReader` no es thread-safe; al usarlo junto con `taskExecutor`, se
envuelve en un `SynchronizedItemStreamReader` para evitar condiciones de carrera entre
hilos. Spring Batch además advierte en el log que los datos de reinicio (restart) de un
`ItemStream` pueden no ser exactos en modo multihilo — limitación conocida y aceptada para
el alcance de este proyecto, ya que no se contempla reanudar Jobs interrumpidos.

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

El resto de la configuración (URL, usuario, driver) ya está en `application.yml` y no
requiere cambios si tu usuario de MySQL es `root`.

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
`spring.batch.job.name` (y habilitando la ejecución puntual con `spring.batch.job.enabled`,
ya que por defecto está desactivada para evitar que los tres Jobs corran juntos al
levantar la app):

```bash
# Job 1: Reporte de Transacciones Diarias
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=reporteTransaccionesDiariasJob --spring.batch.job.enabled=true"

# Job 2: Cálculo de Intereses Mensuales
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=calculoInteresesMensualesJob --spring.batch.job.enabled=true"

# Job 3: Generación de Estados de Cuenta Anuales
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.batch.job.name=generacionEstadosCuentaAnualesJob --spring.batch.job.enabled=true"
```

Cada corrida imprime en consola un resumen (vía `BatchJobCompletionListener` y
`GenericStepListener`) con la cantidad de registros leídos, escritos y saltados por cada
Step — esa consola es evidencia de ejecución. Al finalizar, la aplicación queda corriendo
como servidor web (Tomcat); se cierra con `Ctrl+C`.

### Opción B — Vía API REST (Postman)

Ejecuta la aplicación normalmente (botón Run sobre `BatchMigracionBancoXyzApplication`),
y con la app corriendo en el puerto 8080, dispara cada Job con una petición `POST` sin
body:

| Job | Endpoint |
|---|---|
| Transacciones diarias | `POST http://localhost:8080/api/batch/transacciones-diarias` |
| Intereses mensuales | `POST http://localhost:8080/api/batch/intereses-mensuales` |
| Estados de cuenta anuales | `POST http://localhost:8080/api/batch/estados-cuenta-anuales` |

```

## 6. Tablas generadas

| Job | Tabla de datos válidos | Tabla de anomalías | Tabla de resultado/resumen |
|---|---|---|---|
| 1 | `transacciones` | `transacciones_anomalias` | `resumen_transacciones_diarias` |
| 2 | `cuentas_intereses` (incluye saldo final) | `cuentas_intereses_anomalias` | — |
| 3 | `movimientos_anuales` | `movimientos_anuales_anomalias` | `estados_cuenta_anuales` |

