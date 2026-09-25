# Electrocasa - Pipeline de Datos en Azure Databricks

## 1. Descripcion del caso

Electrocasa integra seis fuentes de informacion: ventas por sucursal, catalogo de productos, empleados de RRHH, resenas, devoluciones y tracking de envios.

El objetivo del proyecto fue construir un flujo de datos de punta a punta en Azure Databricks utilizando una arquitectura Medallion, separando la informacion en capas Bronze, Silver y Gold. Ademas del procesamiento, el proyecto incorpora calidad de datos, historizacion de empleados, quarantine, orquestacion, monitoreo, gobierno y despliegue mediante Declarative Automation Bundles.

```text
Fuentes
  |
  v
Bronze -> Silver -> Gold
             |
             +-> Expectations / Quarantine
             +-> CDC SCD Type 2
  |
  v
Lakeflow Job -> validacion -> monitoreo
  |
  v
Bundle -> targets
```

### Evidencia 1 - Pipeline completo

> docs/images/spd_completo.png

*Ejecución completa del Lakeflow Declarative Pipeline de Electrocasa.*

## 2. Decisiones de ingesta

Una de las partes mas importantes del proyecto fue entender que las seis fuentes no debian tratarse necesariamente de la misma manera. La decision dependio principalmente de como llega la informacion, cuanto cambia y si tenia sentido procesarla incrementalmente o tomar una fotografia completa.

| Fuente | Implementacion | Decision |
|---|---|---|
| Ventas | Auto Loader, CSV, `readStream` | Son archivos diarios de las 40 sucursales y aumentan con el tiempo. Se utiliza una lectura incremental y se conserva `source_file` e `ingestion_at`. |
| Resenas | Auto Loader, JSON, `readStream` | Llegan como archivos JSON y contienen estructuras como tags y respuestas. Se mantienen incrementales y se aplica un schema explicito. |
| Devoluciones | Auto Loader, CSV, `readStream` | Son nuevos eventos que pueden incorporarse progresivamente sin releer todo el historico. |
| Empleados RRHH | Auto Loader, CSV, `readStream` | Los archivos representan eventos de empleados. La lectura incremental permite alimentar posteriormente el CDC de Silver. |
| Productos | Lectura batch JSON + Materialized View | Es un catalogo maestro. En la implementacion actual se toma una fotografia del conjunto y se recalcula cuando corre el pipeline. |
| Tracking | JDBC desde Azure SQL + Materialized View | La informacion vive en Azure SQL. En la version actual se consulta por JDBC y se materializa la fotografia disponible durante la ejecucion. |

En las cuatro fuentes basadas en archivos incrementales se definio `schemaLocation`, schemas explicitos y metadata de ingesta. En productos y tracking se eligio una logica batch porque, para el alcance del proyecto, se priorizo disponer de una fotografia consistente de esas fuentes antes que implementar captura incremental desde esos origenes.

### Evidencia 2 - Ingesta Bronze

> **COLOCAR IMAGEN AQUI:** captura de la ejecucion donde se observen las seis tablas `brz_*`.

**Pie sugerido:** *Fuentes Bronze implementadas dentro del Lakeflow Declarative Pipeline.*

---

## 3. Arquitectura Medallion

### Bronze

Bronze mantiene la informacion cercana al origen. Las fuentes de archivos conservan `ingestion_at` y `source_file`, mientras que tracking registra `source_system`.

La intencion fue tener trazabilidad antes de aplicar reglas de negocio.

### Silver

Silver concentra la parte que me resultaba mas familiar desde el lado de transformacion: limpieza, casting, normalizacion, deduplicacion y reglas de calidad.

Entre las transformaciones implementadas se encuentran:

- Homologacion de metodos de pago.
- Normalizacion de categorias, motivos, couriers y estados de entrega.
- Conversion de fechas y tipos numericos.
- Deduplicacion por identificadores de negocio.
- Separacion de respuestas de resenas mediante `posexplode`.
- CDC SCD Type 2 para empleados.
- Tabla consolidada de quarantine.

### Gold

Gold queda orientado a las preguntas de negocio y al consumo analitico, separando las agregaciones de negocio de las tareas de limpieza realizadas en Silver.

### Evidencia 3 - Capas Medallion

> **COLOCAR IMAGEN AQUI:** captura del DAG o listado de tablas donde se distingan Bronze, Silver y Gold.

---

## 4. Calidad de datos: Expectations y Quarantine

Las reglas no se trataron todas de la misma manera. La politica se decidio segun cuanto afecta el dato a las preguntas de negocio.

| Tabla / regla | Politica | Razon |
|---|---|---|
| Ventas: `monto_total > 0` | DROP | Un monto nulo o no positivo invalida el valor de la venta y afectaria directamente los indicadores comerciales. |
| Devoluciones: `monto_reembolso > 0` | DROP | Un reembolso invalido distorsionaria el analisis economico de devoluciones. |
| Resenas: calificacion entre 1 y 5 | DROP | Una calificacion fuera de la escala no es valida para analizar satisfaccion. |
| Resenas: `fecha_resena` informada | WARN | La resena sigue aportando calificacion y comentario, aunque pierde parte de su valor temporal. |
| Productos: `precio_lista > 0` | DROP | El precio es necesario para un catalogo confiable y para analisis economicos posteriores. |
| Empleados: DNI informado | DROP | El DNI forma parte de la validacion de identidad del registro del empleado. |
| Empleados: `fecha_evento` informada | DROP | El CDC necesita ordenar los cambios; sin fecha de evento no es confiable historizar el registro. |
| Tracking: estado valido | DROP | Un estado fuera del dominio definido no permite interpretar correctamente el seguimiento. |
| Tracking: fecha de actualizacion | WARN | El estado puede conservar valor, pero se registra la falta de referencia temporal. |

Para no perder trazabilidad de los registros descartados se construyo `slv_quarantine` como Materialized View. Esta consolida los rechazos de ventas, devoluciones, resenas, empleados, productos y tracking, guardando fuente, motivo, fecha de rechazo y payload.

### Evidencia 4 - Expectations

> **COLOCAR IMAGEN AQUI:** captura del Pipeline mostrando Expectations `met/unmet` de las tablas Silver.

### Evidencia 5 - Quarantine

> **COLOCAR IMAGEN AQUI:** resultado de una consulta a `slv_quarantine`, preferiblemente mostrando `source_table`, `motivo_rechazo` y `fecha_rechazo`.

---

## 5. Empleados: CDC y SCD Type 2

RRHH fue tratado de manera diferente porque no solo interesaba conocer el empleado actual, sino conservar su historia.

Primero se construye `view_empleados`, donde se estandarizan los eventos y se validan `dni` y `fecha_evento`. Luego `create_auto_cdc_flow` utiliza `id_empleado` como clave y `fecha_evento` como secuencia para construir `slv_empleados_hist` con SCD Type 2.

Finalmente, `slv_empleados_actual` toma los registros cuyo `__END_AT` es nulo y excluye los eventos de baja. De esta forma se dispone de historia y fotografia actual sin mantener dos logicas independientes.

### Evidencia 6 - SCD Type 2

> **COLOCAR IMAGEN AQUI:** consulta de `slv_empleados_hist` donde se vean `__START_AT`, `__END_AT` y diferentes versiones de un empleado.

---

## 6. Pipeline, Job y Bundle

Otro aprendizaje importante fue entender la diferencia entre las piezas del despliegue.

`databricks.yml` define el Bundle y la configuracion por target. Los archivos de `resources` contienen los recursos que Databricks valida y despliega. Entre esos recursos se encuentran el Lakeflow Declarative Pipeline y el Job.

En el proyecto lo entiendo de esta manera:

```text
databricks.yml
      |
      v
resources/*.yml
      |
      +--> spd_dbelectrocasa   -> procesamiento
      |
      +--> electrocasa_job     -> orquestacion
                    |
                    +--> ejecutar_pipeline
                    +--> validar_gold
                    +--> monitorear_calidad
```

El Job funciona como orquestador. Primero ejecuta el pipeline, luego valida Gold y finalmente ejecuta el monitoreo definido para la solucion.

### Evidencia 7 - Bundle desplegado DEV

> **COLOCAR IMAGEN AQUI:** `Bundle resources` o `Deployment output` del target DEV mostrando Pipeline y Job.

### Evidencia 8 - Segundo target

> **COLOCAR IMAGEN AQUI:** deployment exitoso y recursos del segundo target utilizado para cumplir el requisito de despliegue en al menos dos ambientes.

### Evidencia 9 - Job

> **COLOCAR IMAGEN AQUI:** Run exitoso de `electrocasa_job` mostrando las tareas `ejecutar_pipeline`, `validar_gold` y `monitorear_calidad`.

---

## 7. Monitoreo

El pipeline se monitorea desde su interfaz y mediante Event Log.

Consulta utilizada como evidencia:

```sql
SELECT
    timestamp,
    event_type,
    level,
    message
FROM event_log('<PIPELINE_ID>')
ORDER BY timestamp DESC
LIMIT 100;
```

Para el Job se utiliza el historial de Runs y, cuando las system tables estan disponibles, `system.lakeflow.job_run_timeline` y `system.lakeflow.job_task_run_timeline`.

### Evidencia 10 - Event Log

> **COLOCAR IMAGEN AQUI:** resultado de la consulta al Event Log correspondiente a una ejecucion real.

### Evidencia 11 - Historial del Job

> **COLOCAR IMAGEN AQUI:** historial de ejecuciones de `electrocasa_job`.

---

## 8. Seguridad y gobierno

Las credenciales externas no deben quedar versionadas en Git. Para la version final del proyecto se plantea utilizar un Secret Scope respaldado por Azure Key Vault y consumir las claves desde `utils/secrets.py` mediante `dbutils.secrets.get`.

Adicionalmente se utilizan Governed Tags para clasificar columnas sensibles de empleados, por ejemplo DNI, email y salario.

> **Importante:** cada persona que despliegue el repositorio debe configurar su propio Secret Scope / Key Vault con las keys esperadas por el codigo.

### Evidencia 12 - Gobierno

> **COLOCAR IMAGEN AQUI:** consulta a `system.information_schema.column_tags` mostrando las columnas clasificadas.

---

## 9. Despliegue

### Prerrequisitos

- Workspace de Azure Databricks con Unity Catalog.
- Catalogo y schemas del ambiente.
- Volume utilizado como landing y ubicacion para `schemaLocation`.
- SQL Warehouse utilizado por las validaciones.
- Secret Scope y secretos requeridos.
- Permisos necesarios sobre los recursos.

### Flujo

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Para el segundo ambiente se utiliza el mismo Bundle cambiando el target:

```bash
databricks bundle validate -t <target>
databricks bundle deploy -t <target>
```

El objetivo es que la logica del proyecto permanezca igual y que catalogos, schemas, rutas y demas configuraciones dependientes del ambiente se resuelvan mediante las variables del Bundle.

---

## 10. Aprendizajes y oportunidades de mejora

Este proyecto fue retador para mi porque actualmente me dedico principalmente al analisis de datos. La parte de transformaciones con Spark me resultaba mas cercana, pero no habia trabajado una plataforma de ingesta y despliegue con esta estructura.

Al inicio tuve que detenerme a entender la arquitectura antes de seguir escribiendo codigo. Necesitaba comprender como se relacionaban `databricks.yml`, los resources, los targets, el Lakeflow Declarative Pipeline y el Job. Con el proyecto termine entendiendo mejor que los resources describen lo que se despliega, el pipeline concentra el procesamiento y el Job actua como orquestador de esa ejecucion y de sus controles posteriores.

La ingesta fue probablemente la parte mas nueva para mi. Tuve que decidir cuando tenia sentido trabajar incrementalmente y cuando una lectura batch era suficiente para el alcance del ejercicio. Esto tambien me hizo pensar mas alla del codigo y considerar la frecuencia real que necesitaria cada fuente.

Por ejemplo, hoy el flujo esta centralizado en el pipeline y puede ejecutarse de manera programada. Sin embargo, desde una perspectiva de negocio no necesariamente quisiera esperar hasta el dia siguiente para revisar las ventas. Podria tener sentido procesarlas varias veces durante el dia para observar como viene evolucionando la operacion, mientras que un catalogo de productos podria requerir una frecuencia mucho menor.

Por ello, una mejora que me gustaria evaluar en una siguiente version seria desacoplar las frecuencias de procesamiento por fuente. Las ventas podrian tener una llegada o ejecucion incremental mas frecuente, mientras otras fuentes se mantendrian en batch o con una periodicidad diferente. La implementacion actual me permitio entender primero el flujo completo antes de aumentar esa complejidad.

Tambien me quedo como aprendizaje profundizar mas en la diferencia entre streaming, procesamiento incremental y full recompute. Durante el proyecto fue una de las decisiones que mas cuestione, especialmente porque las seis fuentes no tienen el mismo comportamiento.

Utilice IA como apoyo para contrastar ideas, solucionar errores y aprender conceptos que inicialmente eran nuevos para mi. Sin embargo, el principal aprendizaje fue que obtener codigo no era suficiente. Para poder terminar el proyecto tuve que entender por que cada pieza estaba ahi, como se conectaba con las demas y que decision estaba tomando en cada caso.

Agradeceria especialmente feedback sobre la estrategia de ingesta y la frecuencia elegida para cada fuente, ya que considero que es el area donde mas podria evolucionar esta solucion en una siguiente iteracion.

---

## 11. Evidencias recomendadas para la entrega

Para mantener el README compacto, las evidencias principales son:

1. Pipeline completo en SUCCESS con Bronze, Silver y Gold.
2. Seis fuentes Bronze dentro del pipeline.
3. Expectations de Silver.
4. Consulta de `slv_quarantine`.
5. Historico SCD Type 2 de empleados.
6. Bundle desplegado en DEV.
7. Bundle desplegado en un segundo target.
8. Job completo en SUCCESS.
9. Consulta al Event Log.
10. Historial de ejecuciones del Job.
11. Governed Tags, si se incluye gobierno como evidencia adicional.

---

## 12. Cierre

Electrocasa me permitio trabajar un flujo de datos completo y no solamente transformaciones aisladas. El resultado integra fuentes con comportamientos distintos, una arquitectura Medallion, controles de calidad, historizacion, quarantine, orquestacion, monitoreo, gobierno y despliegue como codigo.

Mas importante que cerrar todas las posibles mejoras, el proyecto me permitio comprender mejor como se conectan estas piezas dentro de Azure Databricks y me dejo identificados los siguientes pasos que profundizaria en una implementacion productiva.
