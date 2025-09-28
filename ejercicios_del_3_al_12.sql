--EJERCICIO 3: CREAR TABLA ivr_DETAIL
--Se utilizó CRETE OR REPLACE porque en una primera creación me equivoqué en ciertos atributos, por lo que con replace se actualiza la tabla ya creada

CREATE OR REPLACE TABLE keepcoding.ivr_detail AS
  SELECT
    calls.ivr_id AS calls_ivr_id,
    calls.phone_number AS calls_phone_number,
    calls.ivr_result AS calls_ivr_result,
    calls.vdn_label AS calls_vdn_label,
    calls.start_date AS calls_start_date,
    FORMAT_DATE('%Y%m%d', calls.start_date) as calls_start_date_id,
    calls.end_date AS calls_end_date,
    FORMAT_DATE('%Y%m%d', calls.end_date) as calls_end_date_id,
    calls.total_duration AS calls_total_duration,
    calls.customer_segment AS calls_customer_segment,
    calls.ivr_language AS calls_ivr_language,
    calls.steps_module AS calls_steps_module,
    calls.module_aggregation AS calls_module_aggregation,
    modu.module_sequece AS module_sequece,
    modu.module_name AS module_name,
    modu.module_duration AS module_duration,
    modu.module_result AS module_result,
    steps.step_sequence AS step_sequence,
    steps.step_name AS step_name,
    steps.step_result AS step_result,
    steps.step_description_error AS step_description_error,
    steps.document_type AS document_type,
    steps.document_identification AS document_identification,
    steps.customer_phone AS customer_phone,
    steps.billing_account_id AS billing_account_id
  FROM keepcoding.ivr_calls calls
  LEFT JOIN keepcoding.ivr_modules modu 
    ON calls.ivr_id = modu.ivr_id
  LEFT JOIN keepcoding.ivr_steps steps
    ON modu.ivr_id = steps.ivr_id
   AND modu.module_sequece = steps.module_sequece;


--EJERCICIO 4: Generar el campo vdn_aggregation (como se dice generar y no crear, se entiende que no hay que crear esta columna de forma permanente en la tabla, sino que se trata de generarlo en base a una query)

--Primeramente, se asume que cada id es único y por tanto cada llamada es una fila, pero por si acaso, se comprueba:
SELECT
  COUNT(calls_ivr_id) AS total_registros,
  COUNT(DISTINCT calls_ivr_id) AS total_registros_unicos
FROM keepcoding.ivr_detail;
--Los resultados son distintos por lo que hay varios registros (filas) que corresponden a la misma llamada. Esto ha pasado porque aunque en la tabla ivr_calls cada registro de llamada es único, al hacer el join, detallando más atributos de cada llamada, ha hecho que una misma llamada tenga varios registros, seguramente debido a las actualizaciones de los steps/pasos dados dentro de la llamada, respondiento a la 'máquina'.

SELECT
  DISTINCT calls_ivr_id,--como se indica para cada llamada, se entiende que es para cada registro diferenciado.
  calls_vdn_label,
  CASE
    WHEN UPPER(calls_vdn_label) LIKE 'ATC%' THEN 'FRONT' --Atencion de cliente se generaliza en front-row, como primer interacción
    WHEN UPPER(calls_vdn_label) LIKE 'TECH%' THEN 'TECH' --Tech, significa llevar a una especialista,es decir, tecnico especializado
    WHEN UPPER(calls_vdn_label)='ABSORPTION' THEN 'ABSORPTION' --Para absorber llamadas, descongestionar flujos
    ELSE 'RESTO'
  END AS vdn_aggregation
FROM keepcoding.ivr_detail
;

--EJERCICIO 5: Generar los campos document_type y document_identification

/*
En un primer momento se piensa que cada llamada (calls_ivr_id) solo tiene asociado un solo documento y numero de documento, y que esta combinación salía repetida varias veces, en varios regsitros/actulizaciones de cada llamada. Como se quiere asociar cada llamada a una persona, se debe por tanto eliminar aquellas llamadas donde no se ha encontrado la identificación de la persona. Esto puede dar lugar a la eliminacion de muchas calls_ivr_id, porque pueden que nunca hayan llegado a esa step de identificación
*/

SELECT
  calls_ivr_id,
  document_type,
  document_identification,
  COUNT(*) AS number_repeticiones
FROM keepcoding.ivr_detail
WHERE document_type != 'UNKNOWN'
  AND document_identification != 'UNKNOWN'
GROUP BY 1, 2, 3;

/*
Se veía así que algunas combinaciones salían más de una vez. Sin embargo para comprobar que había una unica combinación por cada llamada, se comparó el numero de combinaciones que salían únicas, con el número de calls_ivr_id distintas que salian
*/

WITH unique_combinations AS (
  SELECT
    calls_ivr_id,
    document_type,
    document_identification,
    COUNT(*) AS number_repeticiones
  FROM keepcoding.ivr_detail
  WHERE document_type != 'UNKNOWN'
    AND document_identification != 'UNKNOWN'
  GROUP BY 1, 2, 3
)
SELECT 
COUNT(DISTINCT calls_ivr_id) AS num_unique_calls,
COUNT(*) AS total_combinations
FROM unique_combinations;

/*
Se ve por tanto, que hay 17821 registros de llamadas únicas (y no 21674 como en un principio, pues hay llamadas sin identificacion), pero hay 17825 de combinaciones distintas, es decir, hay varias llamadas con distintos números de identificación asociados. Para ver el numero de docuemntos asociados distintos a cada llamada se hace la sigueinte query
*/

WITH number_documents_in_call AS(
SELECT
  calls_ivr_id,
  COUNT(DISTINCT CONCAT(document_type, '-', document_identification)) AS num_docs
FROM keepcoding.ivr_detail
WHERE document_type != 'UNKNOWN'
    AND document_identification != 'UNKNOWN'
GROUP BY calls_ivr_id
)

SELECT
  num_docs,
  COUNT(num_docs) as number_calls
FROM number_documents_in_call
GROUP BY 1;


/*
Se ve que hay:
Fila	num_docs	number_calls
1	     1	       17817
2	     2	       4

Es decir, hay muchas llamadas asociadas a un documento, mientras que hay otras que se asocian hasta con 2 documentos. Seguramente porque el cliente se equivocó al poner su documento de identidad. Ahora tendriamos que ver cual de los dos documentos es el que deberia estar asociado con la llamada. Primero se creara la CTE con las combinaciones unicas de calls_ivr_id para quitar duplicados, y luego se podrá hacer una parittion para ver cual sería el elegido.
*/

WITH unique_combinations AS (
  SELECT
    calls_ivr_id,
    document_type,
    document_identification,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(calls_ivr_id AS STRING) -- No se puede hacer partition de un FLOAT64, lo pasamos a STRING y no a INT porque es más permisivo
      ORDER BY document_type, document_identification
    ) AS rn,
    COUNT(*) OVER (PARTITION BY CAST(calls_ivr_id AS STRING)) AS docs_per_call
  FROM keepcoding.ivr_detail
  WHERE document_type != 'UNKNOWN'
    AND document_identification != 'UNKNOWN'
  GROUP BY 1, 2, 3
)

SELECT *
FROM unique_combinations
WHERE docs_per_call > 1
ORDER BY calls_ivr_id, rn;

/*
Se ve que existen 3 llamadas realizadas por la misma persona, donde se incluye 2 tipos de DNI. Es cierto que no parece que sea uno la equivocación, y el otro la corrección del anterior. Por lo que nos decimos por dejar ambos, por si uno es el que se identifca en la llamda, y el otro el DNI de la persona que recibe el servicio (como si una hija llama por su madre). Para entonces finalmente tener cada llamada asignada a un documento hacemos
*/

WITH agrupations_document_per_calls AS (
  SELECT
      calls_ivr_id,
      document_type,
      document_identification,
      ROW_NUMBER() OVER (
        PARTITION BY CAST(calls_ivr_id AS STRING) -- No se puede hacer partition de un FLOAT64, lo pasamos a STRING y no a INT porque es más permisivo
        ORDER BY document_type, document_identification
      ) AS rn,
      COUNT(*) OVER (PARTITION BY CAST(calls_ivr_id AS STRING)) AS docs_per_call
  FROM keepcoding.ivr_detail
  WHERE document_type != 'UNKNOWN'
      AND document_identification != 'UNKNOWN'
  GROUP BY 1, 2, 3
  ORDER BY calls_ivr_id
)

SELECT
    calls_ivr_id,
    docs_per_call,
    ARRAY_AGG((document_type, document_identification) ORDER BY rn) AS documents_of_call
FROM agrupations_document_per_calls
GROUP BY 1, 2;


--EJERCICIO 6: Generar el campo customer_phone

SELECT
    calls_ivr_id,
    customer_phone,
    COUNT(DISTINCT customer_phone) AS phones_per_register
FROM keepcoding.ivr_detail
WHERE customer_phone != 'UNKNOWN'
GROUP BY 1,2
--HAVING COUNT(DISTINCT customer_phone) > 1 (si previamente, agregamos esta frase y en el select quitamos customer_phone, y en el group by el 2, vemos que no hay mas de un numero por registro)
ORDER BY calls_ivr_id;

--EJERCICIO 7: Generar el campo billing_account_id (Igual que el anterior)


SELECT
    calls_ivr_id,
    COUNT(DISTINCT billing_account_id) AS bills_per_register
FROM keepcoding.ivr_detail
WHERE billing_account_id != 'UNKNOWN'
GROUP BY 1
HAVING COUNT(DISTINCT billing_account_id) > 1
ORDER BY calls_ivr_id;

/* Con esto se ve que hay algun registro de llamadas que cuentan con mas de un billing_acount_id, es decir, que un cliente al llamar, ha dando mal su cuenta, o incluso que tiene dos cuentas y pregunta la información de ambas?  Volvemos entonces a hacer la partition como en el ejercicio 5 para ver si vemos alguna relación entre las cuentas de las llamadas con mas de 1 cuenta*/
WITH agrupation_bills_per_call AS (
  SELECT
      calls_ivr_id,
      billing_account_id,
      ROW_NUMBER() OVER (
        PARTITION BY CAST(calls_ivr_id AS STRING) -- No se puede hacer partition de un FLOAT64, lo pasamos a STRING y no a INT porque es más permisivo
        ORDER BY billing_account_id
      ) AS rn,
      COUNT(*) OVER (PARTITION BY CAST(calls_ivr_id AS STRING)) AS bills_per_call
  FROM keepcoding.ivr_detail
  WHERE billing_account_id != 'UNKNOWN'
      AND billing_account_id != 'UNKNOWN'
  GROUP BY 1, 2
  ORDER BY calls_ivr_id
)

SELECT
    calls_ivr_id,
    bills_per_call,
    ARRAY_AGG((billing_account_id) ORDER BY rn) AS bills_per_call
FROM agrupation_bills_per_call
WHERE bills_per_call >1 --Se ve con esto que no existe una relación obvia entre el primer y el segundo bill_account_id, por lo que se tratará de clientes con mas de uno.Es por tanto que se agrupan. Esta linea no es necesaria, y sin ella, se cogerian todos los registros donde se ha podido identificar un billing_acount_id que no sea UNKNOWN
GROUP BY 1, 2;

--EJERCICIO 8 Generar el campo masiva_lg

SELECT 
  calls_ivr_id,
  MAX(IF (module_name = 'AVERIA_MASIVA', 1, 0)) AS masiva_lg --Con el if se haria una columna de 1 y 0 según si le moudlo por el que pasa la llamada es averia masiva o no. Y si hay varios, modulo, en cuanto uno sea la averia, ya sera 1, y por tanto coge ese valor, porque es el maximo
FROM keepcoding.ivr_detail
GROUP BY 1
;

--EJERCICIO 9: Generar el campo info_by_phone_lg (COMO ANTES)
SELECT 
  calls_ivr_id,
  MAX (IF (step_name='CUSTOMERINFOBYPHONE.TX' AND step_result = 'OK', 1, 0 )) AS info_by_phone_lg
FROM keepcoding.ivr_detail
GROUP BY 1
;

--EJERCICIO 10: Generar el campo info_by_dni_lg. Se puede hacer como el anterior, otra manera mas enrevesada es

WITH clas_info_DNI AS (
  SELECT 
    calls_ivr_id,
    CASE
      WHEN step_name = 'CUSTOMERINFOBYDNI.TX' AND step_result = 'OK' THEN 1
      ELSE 0
    END AS info_DNI
  FROM `keepcoding.ivr_detail`
)

SELECT 
  calls_ivr_id,
  CASE
    WHEN SUM(info_DNI) >= 1 THEN 1
    ELSE 0
  END AS info_by_dni_lg
FROM clas_info_DNI
GROUP BY calls_ivr_id
;

--EJERCICIO 11: Generar los campos repeated_phone_24H, cause_recall_phone_24H

WITH selection_phone_dates AS (
  SELECT 
  calls_ivr_id,
  calls_phone_number,
  calls_start_date,
  LAG(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date ASC) AS previous_call,
  LEAD(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date ASC) AS next_call
  FROM keepcoding.ivr_detail
  GROUP BY 1,2,3
)

SELECT 
  *,
  IF (previous_call IS NOT NULL AND TIMESTAMP_DIFF(calls_start_date, previous_call, HOUR)<24,1,0) AS repeated_phone_24H,
  IF (next_call IS NOT NULL AND TIMESTAMP_DIFF(next_call,calls_start_date, HOUR)<24,1,0) AS cause_recall_phone_24H
FROM selection_phone_dates
ORDER BY  1,2
;

/*Simplemente hallando el previous y next call, se ve si se ha vuelto a llamar al mismo numero, y en general parece que las llamadas son seguidas en vez de días posteriores, porque tal vez comunica. Aun asi yo entendería más esta columna agrupando por phone_numbers que por registro de llamada, pero se pide registro por llamada */


--EJERCICIO 12: CREAR TABLA DE ivr_summary
--Como en bigQuery se puede crear CTE dentro de Crete TABLE

CREATE OR REPLACE TABLE keepcoding.ivr_summary AS

WITH calls_base AS ( -- asi cogemos el registro de cada llamada con mayor numero de datos
  SELECT
    calls_ivr_id,
    MAX(calls_phone_number) AS calls_phone_number,
    MAX(calls_ivr_result) AS calls_ivr_result,
    MAX(calls_start_date) AS calls_start_date,
    MAX(calls_end_date) AS calls_end_date,
    MAX(calls_total_duration) AS calls_total_duration,
    MAX(calls_customer_segment) AS calls_customer_segment,
    MAX(calls_ivr_language) AS calls_ivr_language,
    MAX(calls_steps_module) AS calls_steps_module,
    MAX(calls_module_aggregation) AS calls_module_aggregation
  FROM keepcoding.ivr_detail
  GROUP BY calls_ivr_id
),

formation_vdn_aggregation AS(
  SELECT
  DISTINCT calls_ivr_id,--como se indica para cada llamada, se entiende que es para cada registro diferenciado.
  calls_vdn_label,
  CASE
    WHEN UPPER(calls_vdn_label) LIKE 'ATC%' THEN 'FRONT' --Atencion de cliente se generaliza en front-row, como primer interacción
    WHEN UPPER(calls_vdn_label) LIKE 'TECH%' THEN 'TECH' --Tech, significa llevar a una especialista,es decir, tecnico especializado
    WHEN UPPER(calls_vdn_label)='ABSORPTION' THEN 'ABSORPTION' --Para absorber llamadas, descongestionar flujos
    ELSE 'RESTO'
  END AS vdn_aggregation
  FROM keepcoding.ivr_detail
),

documents_per_register AS (
  WITH agrupations_document_per_calls AS (
    SELECT
        calls_ivr_id,
        document_type,
        document_identification,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(calls_ivr_id AS STRING) -- No se puede hacer partition de un FLOAT64, lo pasamos a STRING y no a INT porque es más permisivo
          ORDER BY document_type, document_identification
        ) AS rn,
        COUNT(*) OVER (PARTITION BY CAST(calls_ivr_id AS STRING)) AS docs_per_call
    FROM keepcoding.ivr_detail
    WHERE document_type != 'UNKNOWN'
        AND document_identification != 'UNKNOWN'
    GROUP BY 1, 2, 3
    ORDER BY calls_ivr_id
  )

  SELECT
    calls_ivr_id,
    docs_per_call,
    ARRAY_AGG(document_type ORDER BY rn) AS documents_types,
    ARRAY_AGG(document_identification ORDER BY rn) AS document_identifications --Nos decidimos quedar con ambas documentos, si no podrias seleccionar uno con el último dicho asumiendo que es el correcto ordenando según date
  FROM agrupations_document_per_calls
  GROUP BY 1, 2
),

number_customers AS (
  SELECT
    calls_ivr_id,
    customer_phone,
    COUNT(DISTINCT customer_phone) AS phones_per_register
  FROM keepcoding.ivr_detail
  WHERE customer_phone != 'UNKNOWN'
  GROUP BY 1,2
  --HAVING COUNT(DISTINCT customer_phone) > 1 (si previamente, agregamos esta frase y en el select quitamos customer_phone, y en el group by el 2, vemos que no hay mas de un numero por registro)
  ORDER BY calls_ivr_id
),

bills_per_call AS(
  WITH agrupation_bills_per_call AS (
    SELECT
        calls_ivr_id,
        billing_account_id,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(calls_ivr_id AS STRING)
          ORDER BY billing_account_id
        ) AS rn,
        COUNT(*) OVER (PARTITION BY CAST(calls_ivr_id AS STRING)) AS bills_per_call
    FROM keepcoding.ivr_detail
    WHERE billing_account_id != 'UNKNOWN'
        AND billing_account_id != 'UNKNOWN'
    GROUP BY 1, 2
    ORDER BY calls_ivr_id)

  SELECT
      calls_ivr_id,
      bills_per_call,
      ARRAY_AGG((billing_account_id) ORDER BY rn) AS bills_per_register
  FROM agrupation_bills_per_call
  GROUP BY 1, 2
),

has_passed_MASIVA AS (
  SELECT 
  calls_ivr_id,
  MAX(IF (module_name = 'AVERIA_MASIVA', 1, 0)) AS masiva_lg 
  FROM keepcoding.ivr_detail
  GROUP BY 1
),

info_phone AS(
  SELECT 
  calls_ivr_id,
  MAX (IF (step_name='CUSTOMERINFOBYPHONE.TX' AND step_result = 'OK' AND customer_phone != 'UNKNOWN', 1, 0 )) AS info_by_phone_lg -- se añadio customer_phone porque se vio luego que había filas donde no se registraba customer_phone pero la info_lag salía como 1. tal vez se identifico pero nunca se llego a guardar el numero.
  FROM keepcoding.ivr_detail
  GROUP BY 1
),

info_DNI AS(
  SELECT 
  calls_ivr_id,
  MAX (IF (step_name='CUSTOMERINFOBYDNI.TX' AND step_result = 'OK', 1, 0 )) AS info_by_dni_lg
  FROM keepcoding.ivr_detail
  GROUP BY 1
),

phone_and_dates AS(
  WITH selection_phone_dates AS (
    SELECT 
    calls_ivr_id,
    calls_phone_number,
    calls_start_date,
    LAG(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date ASC) AS previous_call,
    LEAD(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date ASC) AS next_call
    FROM keepcoding.ivr_detail
    GROUP BY 1,2,3
  )

  SELECT 
    *,
    IF (previous_call IS NOT NULL AND TIMESTAMP_DIFF(calls_start_date, previous_call, HOUR)<24,1,0) AS repeated_phone_24H,
    IF (next_call IS NOT NULL AND TIMESTAMP_DIFF(next_call,calls_start_date, HOUR)<24,1,0) AS cause_recall_phone_24H
  FROM selection_phone_dates
  ORDER BY  1,2
)

SELECT 
  base.calls_ivr_id AS ivr_id,
  base.calls_phone_number AS phone_number,
  base.calls_ivr_result AS ivr_result,
  agregation.vdn_aggregation AS vdn_aggregation,
  base.calls_start_date AS start_date,
  base.calls_end_date AS end_date,
  base.calls_total_duration AS total_duration,
  base.calls_customer_segment AS customer_segment,
  base.calls_ivr_language AS ivr_language,
  base.calls_steps_module AS steps_module,
  base.calls_module_aggregation AS module_aggregation,
  documents.documents_types AS document_type,
  documents.document_identifications AS document_identification,
  phones.customer_phone AS customer_phone,
  bills.bills_per_register AS billing_account_id,
  masiva.masiva_lg AS masiva_lg,
  info_phone.info_by_phone_lg AS info_by_phone_lg,
  info_dni.info_by_dni_lg AS info_by_dni_lg,
  phone_and_dates.repeated_phone_24H AS repeated_phone_24H,
  phone_and_dates.cause_recall_phone_24H AS cause_recall_24H

FROM calls_base base
LEFT JOIN formation_vdn_aggregation AS agregation
  ON base.calls_ivr_id = agregation.calls_ivr_id
LEFT JOIN documents_per_register AS documents
  ON base.calls_ivr_id = documents.calls_ivr_id
LEFT JOIN number_customers AS phones
  ON base.calls_ivr_id = phones.calls_ivr_id
LEFT JOIN bills_per_call AS bills
  ON base.calls_ivr_id = bills.calls_ivr_id
LEFT JOIN has_passed_MASIVA AS masiva
  ON base.calls_ivr_id = masiva.calls_ivr_id
LEFT JOIN info_phone 
  ON base.calls_ivr_id = info_phone.calls_ivr_id
LEFT JOIN info_dni
  ON base.calls_ivr_id = info_dni.calls_ivr_id
LEFT JOIN phone_and_dates
  ON base.calls_ivr_id = phone_and_dates.calls_ivr_id
;


--2 metodo

CREATE OR REPLACE TABLE keepcoding.ivr_summary_SECOND_METHOD AS
WITH ranked_calls AS (
  SELECT   
    calls_ivr_id AS ivr_id,
    calls_phone_number AS phone_number,
    calls_ivr_result AS ivr_result,
    calls_start_date AS start_date,
    calls_end_date AS end_date,
    calls_total_duration AS total_duration,
    calls_customer_segment AS customer_segment,
    calls_ivr_language AS ivr_language,
    calls_steps_module AS steps_module,
    calls_module_aggregation AS module_aggregation,
    customer_phone,
    billing_account_id,
    ROW_NUMBER() OVER (
      PARTITION by CAST(calls_ivr_id AS STRING)
      ORDER BY 
        -- sumamos 1 por cada campo válido
        IF(calls_phone_number IS NOT NULL AND calls_phone_number != 'UNKNOWN', 1, 0) +
        IF(customer_phone IS NOT NULL AND customer_phone != 'UNKNOWN', 1, 0) +
        IF(billing_account_id IS NOT NULL AND billing_account_id != 'UNKNOWN', 1, 0) 
      DESC
    ) AS rn
  FROM keepcoding.ivr_detail
),

formation_vdn_aggregation AS(
  SELECT
  DISTINCT calls_ivr_id,--como se indica para cada llamada, se entiende que es para cada registro diferenciado.
  calls_vdn_label,
  CASE
    WHEN UPPER(calls_vdn_label) LIKE 'ATC%' THEN 'FRONT' --Atencion de cliente se generaliza en front-row, como primer interacción
    WHEN UPPER(calls_vdn_label) LIKE 'TECH%' THEN 'TECH' --Tech, significa llevar a una especialista,es decir, tecnico especializado
    WHEN UPPER(calls_vdn_label)='ABSORPTION' THEN 'ABSORPTION' --Para absorber llamadas, descongestionar flujos
    ELSE 'RESTO'
  END AS vdn_aggregation
  FROM keepcoding.ivr_detail
),

documents_per_register AS (
  WITH agrupations_document_per_calls AS (
    SELECT
        calls_ivr_id,
        document_type,
        document_identification,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(calls_ivr_id AS STRING) -- No se puede hacer partition de un FLOAT64, lo pasamos a STRING y no a INT porque es más permisivo
          ORDER BY document_type, document_identification
        ) AS rn,
        COUNT(*) OVER (PARTITION BY CAST(calls_ivr_id AS STRING)) AS docs_per_call
    FROM keepcoding.ivr_detail
    WHERE document_type != 'UNKNOWN'
        AND document_identification != 'UNKNOWN'
    GROUP BY 1, 2, 3
    ORDER BY calls_ivr_id
  )

  SELECT
    calls_ivr_id,
    docs_per_call,
    ARRAY_AGG(document_type ORDER BY rn) AS documents_types,
    ARRAY_AGG(document_identification ORDER BY rn) AS document_identifications --Nos decidimos quedar con ambas documentos, si no podrias seleccionar uno con el último dicho asumiendo que es el correcto ordenando según date
  FROM agrupations_document_per_calls
  GROUP BY 1, 2
),

number_customers AS (
  SELECT
    calls_ivr_id,
    customer_phone,
    COUNT(DISTINCT customer_phone) AS phones_per_register
  FROM keepcoding.ivr_detail
  WHERE customer_phone != 'UNKNOWN'
  GROUP BY 1,2
  --HAVING COUNT(DISTINCT customer_phone) > 1 (si previamente, agregamos esta frase y en el select quitamos customer_phone, y en el group by el 2, vemos que no hay mas de un numero por registro)
  ORDER BY calls_ivr_id
),

bills_per_call AS(
  WITH agrupation_bills_per_call AS (
    SELECT
        calls_ivr_id,
        billing_account_id,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(calls_ivr_id AS STRING)
          ORDER BY billing_account_id
        ) AS rn,
        COUNT(*) OVER (PARTITION BY CAST(calls_ivr_id AS STRING)) AS bills_per_call
    FROM keepcoding.ivr_detail
    WHERE billing_account_id != 'UNKNOWN'
        AND billing_account_id != 'UNKNOWN'
    GROUP BY 1, 2
    ORDER BY calls_ivr_id)

  SELECT
      calls_ivr_id,
      bills_per_call,
      ARRAY_AGG((billing_account_id) ORDER BY rn) AS bills_per_register
  FROM agrupation_bills_per_call
  GROUP BY 1, 2
),

has_passed_MASIVA AS (
  SELECT 
  calls_ivr_id,
  MAX(IF (module_name = 'AVERIA_MASIVA', 1, 0)) AS masiva_lg 
  FROM keepcoding.ivr_detail
  GROUP BY 1
),

info_phone AS(
  SELECT 
  calls_ivr_id,
  MAX (IF (step_name='CUSTOMERINFOBYPHONE.TX' AND step_result = 'OK' AND customer_phone != 'UNKNOWN', 1, 0 )) AS info_by_phone_lg 
  FROM keepcoding.ivr_detail
  GROUP BY 1
),

info_DNI AS(
  SELECT 
  calls_ivr_id,
  MAX (IF (step_name='CUSTOMERINFOBYDNI.TX' AND step_result = 'OK', 1, 0 )) AS info_by_dni_lg
  FROM keepcoding.ivr_detail
  GROUP BY 1
),

phone_and_dates AS(
  WITH selection_phone_dates AS (
    SELECT 
    calls_ivr_id,
    calls_phone_number,
    calls_start_date,
    LAG(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date ASC) AS previous_call,
    LEAD(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date ASC) AS next_call
    FROM keepcoding.ivr_detail
    GROUP BY 1,2,3
  )

  SELECT 
    *,
    IF (previous_call IS NOT NULL AND TIMESTAMP_DIFF(calls_start_date, previous_call, HOUR)<24,1,0) AS repeated_phone_24H,
    IF (next_call IS NOT NULL AND TIMESTAMP_DIFF(next_call,calls_start_date, HOUR)<24,1,0) AS cause_recall_phone_24H
  FROM selection_phone_dates
  ORDER BY  1,2
)
SELECT
  ranked_calls.ivr_id AS ivr_id,
  ranked_calls.phone_number AS phone_number,
  ranked_calls.ivr_result AS ivr_result,
  agregation.vdn_aggregation AS vdn_aggregation,
  ranked_calls.start_date AS start_date,
  ranked_calls.end_date AS end_date,
  ranked_calls.total_duration AS total_duration,
  ranked_calls.customer_segment AS customer_segment,
  ranked_calls.ivr_language AS ivr_language,
  ranked_calls.steps_module AS steps_module,
  ranked_calls.module_aggregation AS module_aggregation,
  documents.documents_types AS document_type,
  documents.document_identifications AS document_identification,
  phones.customer_phone AS customer_phone,
  bills.bills_per_register AS billing_account_id,
  masiva.masiva_lg AS masiva_lg,
  info_phone.info_by_phone_lg AS info_by_phone_lg,
  info_dni.info_by_dni_lg AS info_by_dni_lg,
  phone_and_dates.repeated_phone_24H AS repeated_phone_24H,
  phone_and_dates.cause_recall_phone_24H AS cause_recall_24H
FROM ranked_calls
LEFT JOIN formation_vdn_aggregation AS agregation
  ON ranked_calls.ivr_id = agregation.calls_ivr_id
LEFT JOIN documents_per_register AS documents
  ON ranked_calls.ivr_id = documents.calls_ivr_id
LEFT JOIN number_customers AS phones
  ON ranked_calls.ivr_id = phones.calls_ivr_id
LEFT JOIN bills_per_call AS bills
  ON ranked_calls.ivr_id = bills.calls_ivr_id
LEFT JOIN has_passed_MASIVA AS masiva
  ON ranked_calls.ivr_id = masiva.calls_ivr_id
LEFT JOIN info_phone 
  ON ranked_calls.ivr_id = info_phone.calls_ivr_id
LEFT JOIN info_dni
  ON ranked_calls.ivr_id = info_dni.calls_ivr_id
LEFT JOIN phone_and_dates
  ON ranked_calls.ivr_id = phone_and_dates.calls_ivr_id
WHERE rn = 1;

  



