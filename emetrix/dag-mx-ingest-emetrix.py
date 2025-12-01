#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""DAG con dos metodos: uno ejecuta plantillas flexibles de dataflow para extraer datos desde MYSQL hacia BIGQUERY.
    El segundo metodo consulta la base de datos origen para extraer los ids y el hash de todos los registros por tabla
    para comparar, extraer solo lo que hace falta y borrar los registros que ya no se encuentran en el origen desde un
    procedimiento almacenado en BIGQUERY. Hace una iteracion para multiples tablas.
"""

from __future__ import annotations
import logging
from pathlib import Path
from airflow import DAG
from datetime import timedelta,datetime
from sqlalchemy import create_engine
from google.cloud import bigquery
import pandas as pd
import pandas_gbq

from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.dataflow import  DataflowStartFlexTemplateOperator
from airflow.models.connection import Connection
from airflow.utils.task_group import TaskGroup

import pendulum

PROJECT_ID = 'cc-data-analytics-prd'
DATASET_SINK = 'DS_CDC_EMETRIX'
DATASET_STG = 'DS_CDC_EMETRIX'

MACHINE_TYPE = "n2-standard-2"

NET = "projects/cc-int-shdvpc-prd/global/networks/cc-vpc-internal-01-prd"
SUBNET = "https://www.googleapis.com/compute/v1/projects/cc-int-shdvpc-prd/regions/us-central1/subnetworks/cc-subnet-imternal-us-central1-01-prd"

SCHEMA_LOCATION = 'gs://bkt-dwh-composer/schemas/emetrix_mysql'

AIRFLOW_TAGS = ['MYSQL','CAPTURAS','EMETRIX','DATAFLOW','LUIS SALAZAR','HASH METHOD','FLEX TEMPLATE','PYTHON AIRFLOW']

START_DATE = days_ago(7)

# SCHEDULE_INTERVAL = '5 4,12,20 * * *'
SCHEDULE_INTERVAL = None
# SCHEDULE_INTERVAL = None

default_args = {
    'owner': 'CUERVO-IT',
    'email': 'luis.salazar@it-seekers.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 0,
    'depends_on_past': False,
    'pool': 'emetrix'
}

# Get the current date and time
current_datetime = pendulum.now("America/Mexico_City")
# Calculate the date one month ago
days_ago = current_datetime.subtract(days=10)
formatted_datetime = days_ago.to_datetime_string()

MYSQL_CONN = Connection.get_connection_from_secrets("mysql_emetrix_conn")

LIST_TABLES = [
    [
        1,'capturas_cuervo',
        'HASH_DELTA','DISABLED',
        'id',
        [
            'id',
            'idProyecto',
            'idUsuario',
            'determinanteGSP',
            'fecha',
            'tipoEnvio',
            'idProducto',
            'bool',
            'precioCopa',
            'precioBotella',
            'marca',
            'fechaInicio',
            'fechaFin',
            'tipo',
            'detalle',
            'comentarios',
            'observaciones',
            'aleatorio',
            'ultimaCaptura',
            'activo',
            'leido',
            'fechaCalendario',
            'textoCalendario',
            'latitud',
            'longitud'
        ]
    ],
    [
        2,'capturas_cuervo_backup_archivos',
        'HASH_DELTA','DISABLED',
        'id',
        [
            'id', 
            'idArchivo', 
            'idUsuario', 
            'determinanteGSP', 
            'tipo', 
            'urlArchivo',
            'nombre', 
            'extension', 
            'contenido', 
            'fechaCaptura', 
            'fechaBackup'
        ]
    ],
    [
        3,'capturas_cuervo_convenios',
        'HASH_DELTA','ENABLED',
        'idConvenio',
        [
            'idConvenio', 
            'idEjecutivo', 
            'tipo', 
            'numero', 
            'fechaInicio',
            'fechaFin', 
            'urlPDF', 
            'estatus', 
            'activo', 
            'nivelAutorizacion',
            'autorizacionJefe', 
            'autorizacionJefe_fecha', 
            'autorizacionGerente',
            'autorizacionGerente_fecha', 
            'idUsuarioCarga', 
            'fechaCaptura'
        ]
    ],
    [
        4,'capturas_cuervo_conveniosdetalles',
        'HASH_DELTA','ENABLED',
        'id',
        [
            'id', 
            'idConvenio', 
            'idEjecutivo', 
            'determinanteGSP', 
            'tipo', 
            'subtipo',
            'valorId', 
            'valorNum', 
            'valor1', 
            'valor2', 
            'valor3', 
            'valor4', 
            'valor5',
            'valor6',
            'orden'
        ]
    ],
    [
        5,'capturas_cuervo_xmls_bonificaciones',
        'HASH_DELTA','ENABLED',
        'idBonificacion',
        [
            'idBonificacion', 
            'idConvenio', 
            'idEjecutivo', 
            'determinanteGSP',
            'idCompra', 
            'idCompraProducto', 
            'claveUnica', 
            'nivelProducto',
            'idProducto', 
            'producto', 
            'porcentajeBonificacion', 
            'fechaCompra',
            'meta', 
            'avance', 
            'avanceImporte', 
            'tipo', 
            'bonificacion', 
            'estatus',
            'entregado', 
            'activo', 
            'fechaCaptura'
        ]
    ],
    [
        6,'capturas_cuervo_xmls_catalogos',
        'HASH_DELTA','ENABLED',
        'id',
        [
            'id', 
            'tipo', 
            'valorId', 
            'valorTxt', 
            'orden'
        ]
    ],
    [
        7,'capturas_cuervo_xmls_compras',
        'HASH_DELTA','ENABLED',
        'idCompra',
        [
            'idCompra', 
            'idUsuarioCarga', 
            'idEjecutivo', 
            'determinanteGSP',
            'folioCarga', 
            'tipo', 
            'urlArchivo', 
            'sucursal', 
            'numTicket',
            'numFactura', 
            'folioFiscal', 
            'RFCemisor', 
            'RFCemisorNombre',
            'RFCreceptor', 
            'RFCreceptorNombre', 
            'estatusFactura', 
            'fechaCompra',
            'totalProductos', 
            'totalProductosCuervo', 
            'subtotal', 
            'imp002',
            'imp003', 
            'subtotal003', 
            'totalFactura', 
            'total', 
            'totalCuervo',
            'totalCuervo003', 
            'estatus', 
            'comentarios', 
            'url', 
            'activo',
            'finalizado', 
            'fechaFinalizado', 
            'bonificado', 
            'fechaBonificado',
            'idUsuarioFinalizado', 
            'fechaCaptura', 
            'fechaCarga'
        ]
    ],
    [
        8,'capturas_cuervo_xmls_comprasproductos',
        'HASH_DELTA','ENABLED',
        'id',
        [
            'id', 
            'idCompra', 
            'cruce', 
            'cuervo', 
            'claveUnica', 
            'cantidad', 
            'clave',
            'descripcion', 
            'noIdentificacion', 
            'tipoUnidad', 
            'unidad', 
            'capacidad',
            'cajas9l', 
            'precioUnitario', 
            'imp002', 
            'imp003', 
            'importe',
            'importe003', 
            'descuento', 
            'bonificacion', 
            'manual'
        ]
    ],
    [
        9,'capturas_cuervo_xmls_edocuenta',
        'HASH_DELTA','ENABLED',
        'idOperacion',
        [
            'idOperacion', 
            'idEjecutivo', 
            'determinanteGSP', 
            'idCompra', 
            'idSalida',
            'fecha', 
            'saldoAnterior', 
            'monto', 
            'montoEntregado', 
            'saldo', 
            'accion',
            'fechaCaptura', 
            'activo'
        ]
    ],
    [
        10,'capturas_cuervo_xmls_productos',
        'HASH_DELTA','ENABLED',
        'id',
        [
            'id', 
            'claveUnica', 
            'cuervo', 
            'categoriaId', 
            'categoria',
            'subcategoria', 
            'compania', 
            'familiaId', 
            'familia', 
            'marcaId', 
            'marca',
            'submarcaId', 
            'submarca', 
            'materialId', 
            'material', 
            'capacidad',
            'botellasCN', 
            'tipoCompetencia'
        ]
    ],
    [
        11,'capturas_cuervo_xmls_rfcs',
        'HASH_DELTA','ENABLED',
        'id',
        [
            'id', 
            'rfc', 
            'tipo', 
            'grupo', 
            'nombre', 
            'grupoMayorista',
            'mayoristaAutorizado'
        ]
    ],
    [
        12,'capturas_cuervo_xmls_salidasproductos',
        'HASH_DELTA','ENABLED',
        'idSalida',
        [
            'idSalida', 
            'determinanteGSP', 
            'idEjecutivo', 
            'claveUnica',
            'materialId', 
            'fechaSalida', 
            'fechaCaptura', 
            'tipoSalida', 
            'botellas',
            'costoInterno', 
            'importeInterno', 
            'costoAlzado', 
            'importeAlzado',
            'activo', 
            'idSalidaPt'
        ]
    ],
    [
        13,'capturas_cuervo',
        'FULL','DISABLED',
        """SELECT 
            id,idProyecto,
            idUsuario,
            determinanteGSP,
            CAST(fecha AS CHAR) AS fecha,
            tipoEnvio,
            idProducto,
            bool,
            precioCopa,
            precioBotella,
            marca,
            CAST(fechaInicio AS CHAR) AS fechaInicio,
            CAST(fechaFin AS CHAR) AS fechaFin,
            tipo,
            detalle,
            comentarios,
            observaciones,
            aleatorio,
            ultimaCaptura,
            activo,
            leido,
            CAST(fechaCalendario AS CHAR) AS fechaCalendario,
            textoCalendario,
            latitud,
            longitud 
        FROM capturas_cuervo""",
        'capturas_cuervo_schema.json'
    ],
    [
        14,'capturas_cuervo_backup_archivos',
        'FULL','DISABLED',
        """SELECT
            id, 
            idArchivo, 
            idUsuario, 
            determinanteGSP, 
            tipo, 
            urlArchivo,
            nombre, 
            extension, 
            CAST(contenido AS CHAR) AS contenido, 
            CAST(fechaCaptura AS CHAR) AS fechaCaptura, 
            CAST(fechaBackup AS CHAR) AS fechaBackup
        FROM capturas_cuervo_backup_archivos
        WHERE fechaCaptura > '2025-06-18'""",
        'capturas_cuervo_backup_archivos_schema.json'
    ],
    [
        15,'capturas_cuervo_convenios',
        'FULL','DISABLED',
        """SELECT
            idConvenio, 
            idEjecutivo, 
            tipo, 
            numero, 
            CAST(fechaInicio AS CHAR) AS fechaInicio,
            CAST(fechaFin AS CHAR) AS fechaFin, 
            urlPDF, 
            estatus, 
            activo, 
            nivelAutorizacion,
            autorizacionJefe, 
            CAST(autorizacionJefe_fecha AS CHAR) AS autorizacionJefe_fecha, 
            autorizacionGerente,
            CAST(autorizacionGerente_fecha AS CHAR) AS autorizacionGerente_fecha, 
            idUsuarioCarga, 
            CAST(fechaCaptura AS CHAR) AS fechaCaptura
        FROM capturas_cuervo_convenios""",
        'capturas_cuervo_convenios_schema.json'
    ],
    [
        16,'capturas_cuervo_conveniosdetalles',
        'FULL','DISABLED',
        """SELECT
            id, 
            idConvenio, 
            idEjecutivo, 
            determinanteGSP, 
            tipo, 
            subtipo,
            valorId, 
            valorNum, 
            valor1, 
            valor2, 
            valor3, 
            valor4, 
            valor5,
            orden
        FROM capturas_cuervo_conveniosdetalles""",
        'capturas_cuervo_conveniosdetalles_schema.json'
    ],
    [
        17,'capturas_cuervo_xmls_bonificaciones',
        'FULL','DISABLED',
        """SELECT
            idBonificacion, 
            idConvenio, 
            idEjecutivo, 
            determinanteGSP,
            idCompra, 
            idCompraProducto, 
            claveUnica, 
            nivelProducto,
            idProducto, 
            producto, 
            porcentajeBonificacion, 
            CAST(fechaCompra AS CHAR) AS fechaCompra,
            meta, 
            avance, 
            avanceImporte, 
            tipo, 
            bonificacion, 
            estatus,
            entregado, 
            activo, 
            CAST(fechaCaptura AS CHAR) AS fechaCaptura
        FROM capturas_cuervo_xmls_bonificaciones""",
        'capturas_cuervo_xmls_bonificaciones_schema.json'
    ],
    [
        18,'capturas_cuervo_xmls_catalogos',
        'FULL','DISABLED',
        """SELECT
            id, 
            tipo, 
            valorId, 
            valorTxt, 
            orden
        FROM capturas_cuervo_xmls_catalogos""",
        'capturas_cuervo_xmls_catalogos_schema.json'
    ],
    [
        19,'capturas_cuervo_xmls_compras',
        'FULL','DISABLED',
        """SELECT
            idCompra, 
            idUsuarioCarga, 
            idEjecutivo, 
            determinanteGSP,
            folioCarga, 
            tipo, 
            urlArchivo, 
            sucursal, 
            numTicket,
            numFactura, 
            folioFiscal, 
            RFCemisor, 
            RFCemisorNombre,
            RFCreceptor, 
            RFCreceptorNombre, 
            estatusFactura, 
            CAST(fechaCompra AS CHAR) AS fechaCompra,
            totalProductos, 
            totalProductosCuervo, 
            subtotal, 
            imp002,
            imp003, 
            subtotal003, 
            totalFactura, 
            total, 
            totalCuervo,
            totalCuervo003, 
            estatus, 
            comentarios, 
            url, 
            activo,
            finalizado, 
            CAST(fechaFinalizado AS CHAR) AS fechaFinalizado, 
            bonificado, 
            CAST(fechaBonificado AS CHAR) AS fechaBonificado,
            idUsuarioFinalizado, 
            CAST(fechaCaptura AS CHAR) AS fechaCaptura, 
            CAST(fechaCarga AS CHAR) AS fechaCarga
        FROM capturas_cuervo_xmls_compras""",
        'capturas_cuervo_xmls_compras_schema.json'
    ],
    [
        20,'capturas_cuervo_xmls_comprasproductos',
        'FULL','DISABLED',
        """SELECT
            id, 
            idCompra, 
            cruce, 
            cuervo, 
            claveUnica, 
            cantidad, 
            clave,
            descripcion, 
            noIdentificacion, 
            tipoUnidad, 
            unidad, 
            capacidad,
            cajas9l, 
            precioUnitario, 
            imp002, 
            imp003, 
            importe,
            importe003, 
            descuento, 
            bonificacion, 
            CAST(manual AS CHAR) AS manual
        FROM capturas_cuervo_xmls_comprasproductos""",
        'capturas_cuervo_xmls_comprasproductos_schema.json'
    ],
    [
        21,'capturas_cuervo_xmls_edocuenta',
        'FULL','DISABLED',
        """SELECT
            idOperacion, 
            idEjecutivo, 
            determinanteGSP, 
            idCompra, 
            idSalida,
            CAST(fecha AS CHAR) AS fecha, 
            saldoAnterior, 
            monto, 
            montoEntregado, 
            saldo, 
            accion,
            CAST(fechaCaptura AS CHAR) AS fechaCaptura, 
            CAST(activo AS CHAR) AS activo
        FROM capturas_cuervo_xmls_edocuenta""",
        'capturas_cuervo_xmls_edocuenta_schema.json'
    ],
    [
        22,'capturas_cuervo_xmls_productos',
        'FULL','DISABLED',
        """SELECT
            id, 
            claveUnica, 
            cuervo, 
            categoriaId, 
            categoria,
            subcategoria, 
            compania, 
            familiaId, 
            familia, 
            marcaId, 
            marca,
            submarcaId, 
            submarca, 
            materialId, 
            material, 
            capacidad,
            botellasCN, 
            tipoCompetencia
        FROM capturas_cuervo_xmls_productos""",
        'capturas_cuervo_xmls_productos_schema.json'
    ],
    [
        23,'capturas_cuervo_xmls_rfcs',
        'FULL','DISABLED',
        """SELECT
            id, 
            rfc, 
            tipo, 
            grupo, 
            nombre, 
            CAST(mayoristaAutorizado AS CHAR) AS mayoristaAutorizado
        FROM capturas_cuervo_xmls_rfcs""",
        'capturas_cuervo_xmls_rfcs_schema.json'
    ],
    [
        24,'capturas_cuervo_xmls_salidasproductos',
        'FULL','DISABLED',
        """SELECT
            idSalida, 
            determinanteGSP, 
            idEjecutivo, 
            claveUnica,
            materialId, 
            CAST(fechaSalida AS CHAR) AS fechaSalida, 
            CAST(fechaCaptura AS CHAR) AS fechaCaptura, 
            tipoSalida, 
            botellas,
            costoInterno, 
            importeInterno, 
            costoAlzado, 
            importeAlzado,
            CAST(activo AS CHAR) AS activo, 
            idSalidaPt
        FROM capturas_cuervo_xmls_salidasproductos""",
        'capturas_cuervo_xmls_salidasproductos_schema.json'
    ],
    [
        25,'v_capturas_cuervo_app_fotos',
        'FULL','ENABLED',
        """SELECT 
            id, 
            idProyecto, 
            idVisita, 
            idUsuario, 
            determinanteGSP, 
            foto, 
            idCategoria, 
            comentario, 
            nombre_foto, 
            ruta_foto, 
            tipo, 
            CAST(fecha AS CHAR) AS fecha, 
            otroServer, 
            analizado, 
            CAST(ultActualizacion AS CHAR) AS ultActualizacion, 
            enftp, 
            latitud, 
            longitud
        FROM emetrix.capturas_cuervo_app_fotos""",
        'capturas_cuervo_app_fotos_schema.json'
    ],
    [
        26,'v_capturas_cuervo_app_visitas',
        'FULL','ENABLED',
        """SELECT 
            id, 
            idProyecto, 
            determinanteGSP, 
            idUsuario, 
            CAST(fecha AS CHAR) AS fecha, 
            latitud, 
            longitud, 
            idStatus, 
            latOriginal, 
            lonOriginal, 
            latTienda, 
            lonTienda, 
            movimientos, 
            comentarios1, 
            comentarios2, 
            comentarios3, 
            comentarios4, 
            comentarios5, 
            tiempoTotal, 
            montoGastado, 
            CAST(ultActualizacion AS CHAR) AS ultActualizacion, 
            visitaNombre, 
            incidencia, 
            indicenciaTipo, 
            minutosEnVisitas, 
            minutosEnTraslados, 
            minutosEnDiaTotal, 
            CAST(fechaFin AS CHAR) AS fechaFin, 
            scoreVisita, 
            puntosRecompensas, 
            feedback, 
            aTiempo, 
            fotos, 
            consumo, 
            comentarios, 
            consistencia
        FROM emetrix.capturas_cuervo_app_visitas""",
        'capturas_cuervo_app_visitas_schema.json'
    ],
    [
        27,'v_capturas_cuervo_cat_pop',
        'FULL','ENABLED',
        """SELECT 
            determinanteGSP, 
            sucursal, 
            direccion, 
            latitud, 
            longitud, 
            region
        FROM emetrix.capturas_cuervo_cat_pop""",
        'capturas_cuervo_cat_pop_schema.json'
    ],
    [
        28,'v_capturas_cuervo_cat_usuarios',
        'FULL','ENABLED',
        """SELECT 
            idUsuario, 
            usuario, 
            nombre, 
            apat, 
            cargo, 
            email, 
            telefono, 
            numEmpleado, 
            region
        FROM emetrix.capturas_cuervo_cat_usuarios""",
        'capturas_cuervo_cat_usuarios_schema.json'
    ],
    [
        29,'v_capturas_cuervo_cdcs',
        'FULL','ENABLED',
        """SELECT 
            id, 
            determinanteGSP, 
            popEstado, 
            popCiudad, 
            region, 
            sucursal, 
            tipo, 
            clasificacion, 
            idUsuario, 
            usuario, 
            usuarioNombre, 
            idSuperior1, 
            superior1Nombre, 
            idSuperior2, 
            superior2Nombre, 
            idSuperior3, 
            superior3Nombre
        FROM emetrix.capturas_cuervo_cdcs""",
        'capturas_cuervo_cdcs_schema.json'
    ],
    [
        30,'v_capturas_cuervo_cdcs_tiendas',
        'FULL','ENABLED',
        """SELECT 
            determinanteGSP, 
            popEstado, 
            popCiudad, 
            region, 
            sucursal, 
            tipo, 
            clasificacion
        FROM emetrix.capturas_cuervo_cdcs_tiendas""",
        'capturas_cuervo_cdcs_tiendas_schema.json'
    ],
    [
        31,'v_capturas_cuervo_cdcs_usuarios',
        'FULL','ENABLED',
        """SELECT 
            idUsuario, 
            numEmpleado, 
            usuario, 
            usuarioNombre, 
            nombre, 
            apat, 
            idSuperior1, 
            superior1Nombre, 
            idSuperior2, 
            superior2Nombre, 
            idSuperior3, 
            superior3Nombre, 
            regionEjecutivo
        FROM emetrix.capturas_cuervo_cdcs_usuarios""",
        'capturas_cuervo_cdcs_usuarios_schema.json'
    ],
    [
        32,'v_capturas_cuervo_compras_finalizadas',
        'FULL','ENABLED',
        """SELECT 
            region, 
            regionEjecutivo, 
            idEjecutivo, 
            ejecutivoNombre, 
            ejecutivoUsuario, 
            RFCemisor, 
            RFCemisorNombre, 
            folioEmetrix, 
            sucursal, 
            idCompra, 
            tipoArchivo, 
            folioCarga, 
            CAST(fechaCarga AS CHAR) AS fechaCarga, 
            CAST(fechaCompraCompleto AS CHAR) AS fechaCompraCompleto, 
            CAST(fechaCompra AS CHAR) AS fechaCompra, 
            año AS anio, 
            mes, 
            dia, 
            esCuervo, 
            categoria, 
            subcategoria, 
            compania, 
            familia, 
            marca, 
            submarca, 
            material, 
            capacidad, 
            botellasCN, 
            botellas, 
            cajas9l, 
            cajasNaturales, 
            precioUnitario, 
            importe
        FROM emetrix.capturas_cuervo_compras_finalizadas""",
        'capturas_cuervo_compras_finalizadas_schema.json'
    ],
    [
        33,'capturas_cuervo',
        'TIME_DELTA','ENABLED',
        f"""SELECT 
            id,idProyecto,
            idUsuario,
            determinanteGSP,
            CAST(fecha AS CHAR) AS fecha,
            tipoEnvio,
            idProducto,
            bool,
            precioCopa,
            precioBotella,
            marca,
            CAST(fechaInicio AS CHAR) AS fechaInicio,
            CAST(fechaFin AS CHAR) AS fechaFin,
            tipo,
            detalle,
            comentarios,
            observaciones,
            aleatorio,
            ultimaCaptura,
            activo,
            leido,
            CAST(fechaCalendario AS CHAR) AS fechaCalendario,
            textoCalendario,
            latitud,
            longitud 
        FROM capturas_cuervo
        WHERE fecha >= '{formatted_datetime}'""",
        'capturas_cuervo_schema.json'
    ]
]

def clear_table(tbl_id: str):
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    query=f"TRUNCATE TABLE `{PROJECT_ID}.{tbl_id}`;"
    print(f"running... {query}")
    # try:
    job_config = bigquery.QueryJobConfig()
    query_job = bigquery_client.query(query, job_config=job_config).result()
    print(f"OK -> {query}")
    # except Exception as e:
    #     print(f"Error: {e}")

# def execute_procedure(query_part: str):
#     bigquery_client = bigquery.Client(project=PROJECT_ID)
#     query=f"CALL {query_part}"
#     # try:
#     job_config = bigquery.QueryJobConfig()
#     query_job = bigquery_client.query(query, job_config=job_config).result()
#     print(f"Query executed successfully: {query}", )
#     # except Exception as e:
#     #     print(f"Error ejecutando el procedimiento: {query} >> {e}")

def execute_procedure(query_part: str):
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    query=f"CALL {query_part}"
    print(f"Query a ejecutar:\n{query}\n")
    query_job = bigquery_client.query(
            query, 
            job_config=bigquery.QueryJobConfig()
        )
    results = query_job.result()
    print(f"NÃºmero de registros devueltos: {results.total_rows}")
    bytes_procesados_mb = (query_job.total_bytes_processed or 0) / (1024*1024)
    print(f"Bytes procesados: {query_job.total_bytes_processed} bytes (~{bytes_procesados_mb:.2f} MB)")
    print(f"Â¿Resultados desde cachÃ©?: {query_job.cache_hit}")
    print(f"Job ID: {query_job.job_id}")
    print(f"Hora de inicio: {query_job.started}")
    print(f"Hora de finalizaciÃ³n: {query_job.ended}")

def build_mysql_hash_query(col_list: str, table_name: str, field_key: str):
    casted_col_list = []
    # se itera la lista de columas, agregando el cast
    for col in col_list:
        casted_col_list.append(f"CAST({col} AS CHAR)")
    # se completa la query con el inicio y el final
    mysql_query = f"SELECT {field_key},SHA2(CONCAT_WS('|', {','.join(casted_col_list)} ), 256) AS row_hash FROM {table_name}"
    return mysql_query

def build_bq_comparation_query(col_list: str, table_name: str, field_key: str):
    casted_col_list = []
    # se itera la lista de columas, agregando el cast
    for col in col_list:
        casted_col_list.append(f"CAST({col} AS STRING)")
    # se completa la query con el inicio y el final
    bq_query = f"""WITH BQ AS (SELECT TO_HEX(SHA256(CONCAT({', "|",'.join(casted_col_list)}))) AS row_hash FROM `{PROJECT_ID}.{DATASET_SINK}.{table_name}`) SELECT {field_key} AS ID FROM `{PROJECT_ID}.{DATASET_STG}.MYSQL_HASH_{table_name}` WHERE row_hash NOT IN (SELECT row_hash FROM BQ)"""
    return bq_query

def read_and_write(query: str, table_id: str):
    # Crear engine
    engine = create_engine(f"mysql+mysqlconnector://{MYSQL_CONN.login}:{MYSQL_CONN.password}@{MYSQL_CONN.host}:{MYSQL_CONN.port}/{MYSQL_CONN.schema}")
    print('Sqlalchemy engine OK', engine)
    # ejecuta query
    df = pd.read_sql(query, engine, dtype=str)
    print('Ejecucion de Mysql query Ok')
    print(df.info())
    # escribe restultados
    pandas_gbq.to_gbq(df, table_id, project_id=PROJECT_ID, if_exists='replace')
    print(f'Datos escritos en Bigquery OK: {table_id}')

def read_from_bq(col_list: str, table_name: str, field_key: str):
    # se construye la query de comparacion entre la tabla hash y la tabla destino en bq
    sql_comparation_query = build_bq_comparation_query(col_list, table_name, field_key)
    print(sql_comparation_query)
    # se ejecuta query en bigquery
    df = pandas_gbq.read_gbq(sql_comparation_query, project_id=PROJECT_ID)
    # si el resultado es vacio no se hace nada mas
    if df.empty:
        print('No hay cambios para capturar, Â¡el dataframe regresÃ³ vacio!')
        print(df.head())
        print(f'Se limpia tabla STG: {DATASET_STG}.STG_{table_name}')
        clear_table(f'{DATASET_STG}.STG_{table_name}')
    else:
        # se convierte a string, luego a texto separado por comas y se integra en la query
        df['ID'] = df['ID'].astype(str)
        ids_filtro = df['ID'].str.cat(sep=', ')
        final_query = f'''SELECT * FROM {table_name} WHERE {field_key} IN ({ids_filtro})'''
        print('Query final')
        print(final_query)
        # Se lee en mysql y escribe en bigquery
        read_and_write(final_query, f'{DATASET_STG}.STG_{table_name}')

def mysql_to_bq(col_list:str, table_name:str, field_key: str):
    # se construye la query para obtener el hash desde mysql
    mysql_query = build_mysql_hash_query(col_list,table_name,field_key)
    print('MySQL Query')
    print(mysql_query)
    # Se lee en mysql y escribe en bigquery
    read_and_write(mysql_query,f"{DATASET_STG}.MYSQL_HASH_{table_name}")


with DAG(
    dag_id=Path(__file__).stem,
    default_args=default_args,
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    tags=AIRFLOW_TAGS,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False
    ) as dag:
    # Tareas decorativas
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Iteracion de las tablas
    with TaskGroup(group_id='tbl_group_dataflow', default_args={'pool': 'emetrix'}) as tbl_group_dataflow:
        for tbl_data in LIST_TABLES:
            if tbl_data[2] == 'FULL' and tbl_data[3] == 'ENABLED':
                SQL_QUERY = ' '.join(tbl_data[4].replace('\n',' ').split()) 
                mysql_table = DataflowStartFlexTemplateOperator(  
                    body={
                       "launchParameter": {
                            "job_name": f"mex-emetrix-{tbl_data[0]}-{tbl_data[1].replace('_','-')}",
                            "parameters": {
                                "connectionURL": f"jdbc:mysql://{MYSQL_CONN.host}:{MYSQL_CONN.port}/{MYSQL_CONN.schema}",
                                "outputTable": f"{PROJECT_ID}:{DATASET_SINK}.{tbl_data[1]}",
                                "bigQueryLoadingTemporaryDirectory": "gs://bkt-dwh-composer/temp",
                                "username": MYSQL_CONN.login,
                                "password": MYSQL_CONN.password,
                                "query": SQL_QUERY,
                                "isTruncate": "true",
                                "useColumnAlias": "true",
                                "createDisposition": "CREATE_IF_NEEDED",
                                "bigQuerySchemaPath": f"{SCHEMA_LOCATION}/{tbl_data[5]}"
                            },
                            "environment": {
                                "numWorkers": 1,
                                "maxWorkers": 1,
                                "tempLocation": "gs://bkt-dwh-composer/MEX_SYSTEMS/MYSQL/temp",
                                "machineType": MACHINE_TYPE,
                                "subnetwork": SUBNET,
                                "network": NET,
                                "ipConfiguration": "WORKER_IP_PRIVATE", 
                                "additionalExperiments": [],
                                "workerZone": "us-central1-a"
                            },
                            "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/flex/MySQL_to_BigQuery"
                        }
                    },
                    location = "us-central1",
                    project_id = PROJECT_ID,
                    task_id = f'{tbl_data[0]}-{tbl_data[1]}',
                    append_job_name = "True",
                    retries=1,
                    retry_delay=timedelta(minutes=3)
                )
                mysql_table

    with TaskGroup(group_id='tbl_group_hash', default_args={'pool': 'emetrix'}) as tbl_group_hash:
        for tbl_data in LIST_TABLES:
            if tbl_data[2] == 'HASH_DELTA' and tbl_data[3] == 'ENABLED':
                mysql_hash_to_bq = PythonOperator(
                    task_id=f"mysql_tables_to_bq_{tbl_data[0]}_{tbl_data[1]}",
                    retries=1,
                    retry_delay=timedelta(minutes=3),
                    python_callable=mysql_to_bq,
                    op_kwargs={
                            'table_name': tbl_data[1],
                            'field_key': tbl_data[4],
                            'col_list': tbl_data[5]
                    }
                )
                comparation_from_bq = PythonOperator(
                    task_id=f"comparation_from_bq_{tbl_data[0]}_{tbl_data[1]}",
                    retries=1,
                    retry_delay=timedelta(minutes=3),
                    python_callable=read_from_bq,
                    op_kwargs={
                            'table_name': tbl_data[1],
                            'field_key': tbl_data[4],
                            'col_list': tbl_data[5]
                    }
                )
                compactation_query = PythonOperator(
                    task_id=f"compactation_query_{tbl_data[0]}_{tbl_data[1]}",
                    retries=1,
                    retry_delay=timedelta(minutes=3),
                    python_callable=execute_procedure,
                    op_kwargs={
                            'query_part':f"DS_CDC_EMETRIX.SP_capturas_cuervo({tbl_data[0]},'{formatted_datetime}')"
                    }
                )
                mysql_hash_to_bq >> comparation_from_bq >> compactation_query

    with TaskGroup(group_id='tbl_group_delta', default_args={'pool': 'emetrix'}) as tbl_group_time_delta:
        for tbl_data in LIST_TABLES:
            if tbl_data[2] == 'TIME_DELTA' and tbl_data[3] == 'ENABLED':
                SQL_QUERY = ' '.join(tbl_data[4].replace('\n',' ').split()) 
                mysql_table = DataflowStartFlexTemplateOperator(  
                    body={
                       "launchParameter": {
                            "job_name": f"mex-emetrix-{tbl_data[0]}-{tbl_data[1].replace('_','-')}",
                            "parameters": {
                                "connectionURL": f"jdbc:mysql://{MYSQL_CONN.host}:{MYSQL_CONN.port}/{MYSQL_CONN.schema}",
                                "outputTable": f"{PROJECT_ID}:{DATASET_SINK}.STG_{tbl_data[1]}",
                                "bigQueryLoadingTemporaryDirectory": "gs://bkt-dwh-composer/temp",
                                "username": MYSQL_CONN.login,
                                "password": MYSQL_CONN.password,
                                "query": SQL_QUERY,
                                "isTruncate": "true",
                                "useColumnAlias": "true",
                                "createDisposition": "CREATE_IF_NEEDED",
                                "bigQuerySchemaPath": f"{SCHEMA_LOCATION}/{tbl_data[5]}"
                            },
                            "environment": {
                                "numWorkers": 1,
                                "maxWorkers": 1,
                                "tempLocation": "gs://bkt-dwh-composer/MEX_SYSTEMS/MYSQL/temp",
                                "machineType": MACHINE_TYPE,
                                "subnetwork": SUBNET,
                                "network": NET,
                                "ipConfiguration": "WORKER_IP_PRIVATE", 
                                "additionalExperiments": [],
                                "workerZone": "us-central1-a"
                            },
                            "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/flex/MySQL_to_BigQuery"
                        }
                    },
                    location = "us-central1",
                    project_id = PROJECT_ID,
                    task_id = f'{tbl_data[0]}-{tbl_data[1]}',
                    append_job_name = "True",
                    retries=1,
                    retry_delay=timedelta(minutes=3)
                )
                compactation_query = PythonOperator(
                    task_id=f"compactation_query_{tbl_data[0]}_{tbl_data[1]}",
                    retries=1,
                    retry_delay=timedelta(minutes=3),
                    python_callable=execute_procedure,
                    op_kwargs={
                            'query_part':f"DS_CDC_EMETRIX.SP_capturas_cuervo({tbl_data[0]},'{formatted_datetime}')"
                    }
                )
                mysql_table >> compactation_query

    start >> tbl_group_hash >> tbl_group_dataflow >> tbl_group_time_delta >> end 
