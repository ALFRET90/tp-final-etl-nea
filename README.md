# Pipeline ETL — Exportaciones del NEA (1993–2024)

**Unidad II · Fundamentos de la Programación**
Diplomatura en Data Analytics e IA Aplicada — UNNE / Extender

---

## Qué hace este pipeline

Es un **ETL** en Python puro (solo biblioteca estándar) que arma, desde cero
y en cada corrida, un dataset analítico de las exportaciones de las cuatro
provincias del NEA (Chaco, Corrientes, Formosa y Misiones) entre 1993 y
2024:

```
   API datos.gob.ar          data/raw/*.json         data/processed/
   (INDEC, 8 llamadas)  -->  (crudo, sin tocar) -->  exportaciones_nea.csv
                                                      resumen.json
        EXTRACT                  TRANSFORM              CHEQUEAR + LOAD
```

1. **Extract** (`src/extract.py`) pide a la API de Series de Tiempo de
   datos.gob.ar las series de exportaciones por país de destino (dataset
   357.1) y por rubro (dataset 350.1) de cada provincia, y guarda la
   respuesta cruda en `data/raw/`.
2. **Transform** (`src/transform.py`) convierte ese formato ancho (una
   columna por destino) en formato largo (una fila por año-provincia-destino),
   calcula columnas derivadas (región geoeconómica, década, participación
   %, variación interanual, ranking del destino) y las une con el rubro
   principal de cada provincia-año.
3. **Load** (`src/load.py`) corre 5 controles de calidad sobre el dataset
   final (4 de ellos cortan el pipeline si fallan) y, si todo está bien,
   guarda el CSV final, un resumen en JSON y una línea en el log de
   corridas.

El resultado es `data/processed/exportaciones_nea.csv`: **1.408 filas × 13
columnas** (4 provincias × 11 destinos × 32 años).

---

## Instalación y ejecución

Necesitás **Python 3.8 o superior**. No hay dependencias externas: todo el
código usa `csv`, `json`, `logging`, `os`, `datetime` y `urllib` de la
biblioteca estándar.

```bash
python --version          # verificá que sea 3.8+
python src/main.py        # corre el pipeline completo (necesita internet)
```

La primera corrida descarga los datos de la API y los deja en `data/raw/`.
A partir de ahí podés reprocesar sin conexión:

```bash
python src/main.py --sin-internet
```

Correr los tests:

```bash
python tests/test_transform.py
```

El pipeline es **idempotente**: correrlo varias veces deja el mismo CSV
(se reescribe con `"w"`), mientras que `logs/pipeline.log` va acumulando
una línea por corrida (se abre con `"a"`), para poder auditar cuántas
veces se ejecutó.

---

## De dónde salen los datos

Fuente: **INDEC**, a través de la [API de Series de Tiempo](https://apis.datos.gob.ar/series/api/)
del portal de datos abiertos del Estado argentino (datos.gob.ar). Es una
API pública, no requiere credenciales.

- **Dataset 357.1** — Exportaciones por provincia y país de destino.
- **Dataset 350.1** — Exportaciones por provincia y rubro.

Se consultan 4 provincias (Chaco, Corrientes, Formosa, Misiones), con sus
11 destinos principales cada una (10 países + "Resto") y sus 4 grandes
rubros (Productos primarios, MOA, MOI, Combustibles y Energía), para el
período 1993–2024. La unidad de todos los valores es **millones de
dólares FOB**.

Todas las claves de config (`config.py`) —IDs de series, mapeo de países a
región, parámetros de negocio y umbrales de calidad— están separadas del
código: el pipeline no tiene un solo valor "hardcodeado" en medio de la
lógica.

---

## Qué encontré en los datos

Lo más llamativo al mirar el CSV es el crecimiento de **China** como
destino: en 1993 era un comprador casi irrelevante para las provincias del
NEA (apenas U$S 0,31 millones desde Chaco, U$S 0,13 millones desde
Formosa, y directamente U$S 0 desde Misiones), y para 2024 se convirtió en
uno de los principales destinos de Chaco (U$S 110,93 millones, casi 360
veces más que en 1993) y pasó a exportarle U$S 53,26 millones a Misiones,
partiendo de cero. También noto un perfil bien distinto entre provincias:
en Chaco, Corrientes y Formosa el rubro principal casi todos los años es
"Productos primarios" (soja, algodón, otros granos sin elaborar), mientras
que en Misiones el rubro principal es casi siempre "MOA" —manufacturas de
origen agropecuario—, coherente con una economía más orientada a productos
con algo de procesamiento (yerba mate, té, madera) en lugar de la materia
prima sin elaborar.

---

## Estructura del proyecto

```
├── config.py              Configuración: IDs de series, rutas, mapeos.
├── src/
│   ├── extract.py         Descarga de la API -> data/raw/
│   ├── transform.py       Ancho a largo, columnas derivadas, join con rubros
│   ├── load.py             Quality checks + guardado de CSV/JSON/log
│   └── main.py             Orquesta EXTRACT -> TRANSFORM -> LOAD
├── tests/
│   └── test_transform.py  20 tests (17 del template + 3 propios)
├── data/
│   ├── raw/                Datos crudos (no se versionan)
│   └── processed/          Salidas finales (no se versionan)
└── logs/                   Historial de corridas
```

---

## Los quality checks

`load.py` corre 5 controles antes de publicar el dataset. Cuatro son
críticos (si fallan, el pipeline corta lanzando una excepción en lugar de
entregar un CSV roto):

1. **Cantidad** — ¿hay al menos 1.000 filas?
2. **Columnas** — ¿todas las filas tienen exactamente las 13 columnas del
   contrato?
3. **Unicidad** — ¿la clave (provincia, año, destino) no se repite?
4. **Rangos** — ¿ningún `valor_musd` es negativo o mayor a 10.000 millones
   de USD (un valor así sería un error de los datos, no una exportación
   real)?

El quinto control (**cobertura**) es solo una advertencia: cuenta cuántas
filas quedaron sin variación interanual (esperable en el primer año de
cada serie) y cuántas sin rubro asignado.

---

## Una aclaración sobre el ranking

Al validar contra el ejemplo del enunciado noté una diferencia: el
enunciado muestra a China en el puesto 1 de Chaco 2024 y a Brasil en el
puesto 6, mientras que mi pipeline calcula China en el puesto 2 y Brasil
en el puesto 4 (el resto de los valores —monto exportado, total
provincial, participación % y variación interanual— coinciden
exactamente con el enunciado). La diferencia aparece porque "Resto" tiene
en 2024 un valor más alto que China (U$S 193,75 millones) y mi
`agregar_ranking()` lo incluye como un destino más al ordenar, tal como
indica el contrato de la función. Lo dejo anotado por si la cátedra
prefiere excluir "Resto" del ranking.
