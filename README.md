# Migración de Procesos Batch — Banco XYZ

Proyecto de la Semana 1 de **Desarrollo Backend III (PBY2203)**. 

## 1. Objetivo del proyecto

Modernizar tres procesos batch del sistema legacy del banco, garantizando la integridad y
consistencia de los datos mediante validación, transformación y manejo de errores:

1. **Reporte de Transacciones Diarias** — detecta anomalías y genera un resumen diario.
2. **Cálculo de Intereses Mensuales** — aplica intereses sobre cuentas de ahorro/préstamo/hipoteca
   y actualiza el saldo final.
3. **Generación de Estados de Cuenta Anuales** — compila el historial anual de cada cuenta para
   auditorías.

## 2. Estructura del código

```
src/main/java/cl/duocuc/bancoxyz/
 ├─ BatchMigracionBancoXyzApplication.java   Clase principal (Spring Boot)
 ├─ config/
 │   ├─ JobTransaccionesDiariasConfig.java    Job 1: reader/processor/writer/steps
 │   ├─ JobInteresesMensualesConfig.java      Job 2: reader/processor/writer/steps
 │   └─ JobEstadosCuentaAnualesConfig.java    Job 3: reader/processor/writer/steps
 ├─ model/          Entidades JPA (datos válidos, anomalías y resúmenes) + DTOs de CSV
 ├─ processor/       ItemProcessor de cada Job: valida, corrige y detecta anomalías
 ├─ repository/       Interfaces Spring Data JPA
 └─ listener/
     ├─ GenericSkipListener.java              Captura errores técnicos de lectura/proceso/escritura
     └─ BatchJobCompletionListener.java        Imprime resumen de cada corrida (leídos/escritos/saltados)

src/main/resources/
 ├─ application.yml     Configuración de MySQL y Spring Batch
 └─ data/                CSV de entrada (transacciones.csv, intereses.csv, cuentas_anuales.csv)
```

### Manejo de errores y calidad de datos

Cada `ItemProcessor` aplica las reglas de negocio del proceso y, si una fila no cumple, la
**descarta y la registra** en una tabla `*_anomalias` con el motivo exacto del rechazo (no detiene
el Job). Adicionalmente, `GenericSkipListener` + `faultTolerant().skip(...)` cubren errores
técnicos inesperados (por ejemplo, una fila corrupta que ni siquiera se puede parsear).

| Job | Reglas aplicadas |
|---|---|
| Transacciones diarias | monto > 0, fecha válida (`yyyy-MM-dd` o `yyyy/MM/dd`), tipo débito/crédito, sin duplicados |
| Intereses mensuales | saldo ≥ 0, edad 18–100, tipo ahorro/préstamo/hipoteca, sin `cuenta_id` duplicado |
| Estados de cuenta anuales | fecha válida, descripción no vacía, signo del monto coherente con el tipo (depósito > 0, retiro/compra < 0), sin duplicados |

## 3. Prerrequisitos

- JDK 17+
- Maven 3.9+
- MySQL 8 corriendo localmente

## 4. Configuración de la base de datos

```sql
CREATE DATABASE banco_xyz_batch CHARACTER SET utf8mb4;
```

Ajusta usuario/clave en `src/main/resources/application.yml` si no usas `root/root`.
Al levantar la aplicación, Hibernate (`ddl-auto: update`) crea automáticamente todas las tablas
de negocio, y Spring Batch crea sus propias tablas de metadata (`BATCH_JOB_INSTANCE`,
`BATCH_STEP_EXECUTION`, etc.).

## 5. Cómo ejecutar el proyecto

Compilar:
```bash
mvn clean package
```

Cada Job se dispara de forma independiente indicando su nombre con la propiedad
`spring.batch.job.name`:

```bash
# Job 1: Reporte de Transacciones Diarias
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.batch.job.name=reporteTransaccionesDiariasJob"

# Job 2: Cálculo de Intereses Mensuales
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.batch.job.name=calculoInteresesMensualesJob"

# Job 3: Generación de Estados de Cuenta Anuales
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.batch.job.name=generacionEstadosCuentaAnualesJob"
```

También puedes ejecutar el `.jar` empaquetado:
```bash
java -jar target/batch-migracion-banco-xyz.jar --spring.batch.job.name=reporteTransaccionesDiariasJob
```

Cada corrida imprime en consola un resumen (vía `BatchJobCompletionListener`) con la cantidad de
registros leídos, escritos y saltados por cada Step — esa consola es tu evidencia de ejecución.

## 6. Tablas generadas

| Job | Tabla de datos válidos | Tabla de anomalías | Tabla de resultado/resumen |
|---|---|---|---|
| 1 | `transacciones` | `transacciones_anomalias` | `resumen_transacciones_diarias` |
| 2 | `cuentas_intereses` (incluye saldo final) | `cuentas_intereses_anomalias` | — |
| 3 | `movimientos_anuales` | `movimientos_anuales_anomalias` | `estados_cuenta_anuales` |


