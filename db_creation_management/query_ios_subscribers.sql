--pregunta 1: A partir del archivo **ios_subscriber_data.csv** que contiene data del app store connect desde el 01/01/2023 al 31/12/2023. Crear una tabla en la base de datos
--¿En cuantos países estamos operando desde el App Store?

--Para solo ver los paises de la tabla hago un select distinct de la columna country
select DISTINCT country from ios_subscribers;

--pregunta 2: Del total de trials, ¿Cuales son los totales por países?

--Para ver el total de usuarios que se registraron con un free trial, hago un select count de la columna subs_id 
--y filtro por subscription_offer_type

SELECT DISTINCT country, count(DISTINCT subs_id) FROM ios_subscribers WHERE subscription_offer_type LIKE '%Trial%'
group by 1;

 --pregunta 3: ¿Cúal es el país con mayor porcentaje de comisión en 2023?
 --Para esta pregunta hago un CTE para obtener el total de comisiones por pais y divisa, calculo la comision total 
 --(filtrando los refunds) y luego calculo el porcentaje de comision total

 with commision as (SELECT DISTINCT country, customer_currency, SUM( customer_price_usd)-SUM( developer_proceeds_usd ) 
 as total_commission FROM ios_subscribers WHERE event_type not in ('Refund') 
 and event_date BETWEEN '2023-01-01' AND '2023-12-31'
 group by 1,2),
 total_commission_sum AS (
 SELECT SUM(total_commission) AS overall_total_commission
 FROM commision
 )
 SELECT 
 c.country,
 SUM(c.total_commission) AS total_commission,
((SUM(c.total_commission) / t.overall_total_commission) * 100) AS percentage_of_total
 FROM 
 commision c, total_commission_sum t
 GROUP BY 
 c.country, t.overall_total_commission
 ORDER BY 
 percentage_of_total DESC;

 --pregunta 4: Armar un historial de eventos para cada subscripción (subs_id PK). Se debe visualizar en cada evento los siguientes datos.
 --Para esto hago un select de las columnas subs_id, event_date, plan_id, 
 --customer_price_usd, developer_proceeds_usd, country, units, event_type y luego hago un LAG y LEAD de las columnas 
 --para obtener los valores anteriores y siguientes de las columnas para cada fila.

 SELECT 
 subs_id,
 event_date,
 plan_id,
 customer_price_usd,
 developer_proceeds_usd,
 country,
 units,
 event_type,
 LAG(event_date) OVER (PARTITION BY subs_id ORDER BY event_date) AS previous_event_date,
 LAG(plan_id) OVER (PARTITION BY subs_id ORDER BY event_date) AS previous_plan_id,
 LAG(customer_price_usd) OVER (PARTITION BY subs_id ORDER BY event_date) AS previous_customer_price_usd,
 LAG(developer_proceeds_usd) OVER (PARTITION BY subs_id ORDER BY event_date) AS previous_developer_proceeds_usd,
 LAG(units) OVER (PARTITION BY subs_id ORDER BY event_date) AS previous_units,
 LAG(event_type) OVER (PARTITION BY subs_id ORDER BY event_date) AS previous_event_type,
 LEAD(event_date) OVER (PARTITION BY subs_id ORDER BY event_date) AS next_event_date,
 LEAD(plan_id) OVER (PARTITION BY subs_id ORDER BY event_date) AS next_plan_id,
 LEAD(customer_price_usd) OVER (PARTITION BY subs_id ORDER BY event_date) AS next_customer_price_usd,
 LEAD(developer_proceeds_usd) OVER (PARTITION BY subs_id ORDER BY event_date) AS next_developer_proceeds_usd,
 LEAD(units) OVER (PARTITION BY subs_id ORDER BY event_date) AS next_units,
 LEAD(event_type) OVER (PARTITION BY subs_id ORDER BY event_date) AS next_event_type
 FROM 
 ios_subscribers;
 
 --pregunta 5: Armar una query para mostrar para cada subs_id el primer evento con su tipo y fecha y el último evento con su tipo y fecha.

 --Para hacer esto hago un select de las columnas subs_id, event_type y hago un FIRST_VALUE y LAST_VALUE
 --para obtener el primer y ultimo valor de event_type por cada subs_id
 SELECT 
 subs_id, 
 first_value(event_type) OVER (PARTITION BY subs_id ORDER BY event_date) AS first_event,
 first_value(event_date) OVER (PARTITION BY subs_id ORDER BY event_date) AS first_event_date,
 last_value(event_type) OVER (PARTITION BY subs_id ORDER BY event_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event,
 last_value(event_date) OVER (PARTITION BY subs_id ORDER BY event_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_date
 FROM 
 ios_subscribers;

--pregunta  6: Para cada día y por país calcular la cantidad de trials de 7 días de prueba (subscription_offer_duration*), 
--calcular la cantidad de subscripciones a 10 días a partir de esos trials, 
--calcular la tasa de conversión de Trial a Subscripción a día 10 de los trials de 7 días de prueba, Gross Revenue a día 10. 

--Para visualizar esto hago un CTE para filtrar los datos de los usuarios que se registraron con una oferta de 7 dias,
--luego, hago otro CTE para filtrar los usuarios que se suscribieron en los 10 dias siguientes a la oferta, 
--y finlamente hago un join para combinar los datos y poder visualizar los campos calculados

WITH offer_counts AS (
    SELECT event_date AS offer_event_date,
        country,
        subs_id
    FROM ios_subscribers
    WHERE subscription_offer_duration = '7 Days'
),
charged_counts AS (
    SELECT a.subs_id,
        a.offer_event_date,
        a.country,
        COUNT(DISTINCT b.subs_id) AS total_subscribed,
        b.customer_price
    FROM offer_counts a
        JOIN ios_subscribers b ON a.subs_id = b.subs_id
    WHERE b.event_type = 'New Paid Subscription From Trial'
        AND b.event_date BETWEEN a.offer_event_date AND a.offer_event_date + INTERVAL '10 days'
    GROUP BY a.subs_id,
        a.offer_event_date,
        a.country,
        b.customer_price
)
select distinct o.offer_event_date,
    o.country,
    count(distinct o.subs_id) as free_7dt,
    sum(ch.total_subscribed) as purchase_from_7Dt_10D,
    (sum(ch.total_subscribed) / count(distinct o.subs_id) * 100) as subscription_rate,
    sum(customer_price) as gross_revenue
from offer_counts o
    left join charged_counts ch on o.subs_id = ch.subs_id
    and o.offer_event_date = ch.offer_event_date
    and o.country = ch.country
GROUP BY 1,
    2;

 

---------------------------------------------------------------------------------------------------------------------

