/*
--Pregunta 7: A partir del archivo **investment_all_networks.csv** que contiene data del app store connect 
desde el 01/01/2023 al 31/12/2023. Crear una tabla en la base de datos. Considerando sólo la inversión para el App Store,
¿En cuantos países invertimos? ¿Hay alguna diferencia respecto a los países que operamos en el App Store? */

--Para ver los resultados agrupo por pais y sumo el spend, filtrando por solo los valores de IOS. La respuesta es que se invierte en dos paises mas
--que en la app store: ZA y MX
SELECT DISTINCT COUNTRY, SUM(spend_usd) AS SPEND
FROM investment_all_networks
WHERE platform = 'IOS'
GROUP BY 1;

--Pregunta 8: ¿Cuál es el Share de Investment por país, considerando sólo los países que operamos en el App Store?

--US es el pais de mas inversion
--Para lograr ver esto primero hago un CTE para sumar el spend por pais y luego hago otro CTE para sumar el total de la inversion 
--y luego hago un join para poder calcular el porcentaje de la inversion por pais
WITH SPEND_BY_COUNTRY AS (
    SELECT DISTINCT country, SUM(spend_usd) AS spend
    FROM investment_all_networks
    WHERE platform = 'IOS'
    and country not in ('ZA', 'MX')
    GROUP BY 1
), TOTAL_SPEND AS (SELECT SUM(spend_usd) as total_spend from investment_all_networks WHERE platform = 'IOS')
SELECT DISTINCT a.country, sum(a.spend), sum((a.spend / b.total_spend) * 100) as percentage_of_total
from spend_by_country a, total_spend b
GROUP BY 1;

--Pregunta 9: Para cada día y por país calcular el total de spend, impresiones y clics.

--Para ver esto agrupo por pais, fecha y sumo las impresiones, clicks y costo. Filtro por la plataforma IOS
SELECT DISTINCT country, date, sum(COALESCE(impressions,0)) as impressions, sum(COALESCE(clicks,0)) as clicks, sum(spend_usd) as spend
from investment_all_networks
WHERE platform = 'IOS'
group by 1,2;

--Pregunta: 4. A partir de las queries generadas en los incisos 6 y 9, Calcular ROAS (Retorno del gasto de acquisición), 
--CPM (Costo por mil impresiones), CPC (Costo por clic), y considerar nuestro mercado principal (País más importante), 
----visualizar las métricas de Investment (Investment, CPM, CPC, Costo por Trial CPT), Conversión (T2S 10D) y Perfomance (ROAS), 
--observar y analizar tendencias, mencionar comentarios y/o hipótesis.

-- Para ver esto primero hago un CTE para filtrar los datos de los usuarios que se registraron con una oferta de 7 dias, 
--luego, hago otro CTE para filtrar los usuarios que se suscribieron en los 10 dias siguientes a la oferta, 
--y finalmente hago otro CTE para obtener el recuento total de registros de free trial. Con todo esto hago un join para combinar los datos 
--y poder visualizar los campos calculados

WITH offer_counts AS (
    SELECT event_date AS offer_event_date,
        subscription_offer_type,
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
    WHERE b.status = 'Charged'
        AND b.event_date BETWEEN a.offer_event_date AND a.offer_event_date + INTERVAL '10 days'
    GROUP BY a.subs_id,
        a.offer_event_date,
        a.country,
        b.customer_price
),free_trial_counts AS (
    SELECT 
        o.offer_event_date,
        o.country,
        COUNT(DISTINCT CASE WHEN o.subscription_offer_type = 'Free Trial' THEN o.subs_id ELSE NULL END) AS free_trial_count
    FROM offer_counts o
    GROUP BY o.offer_event_date, o.country
)
select distinct o.country,
    o.offer_event_date,
    count(distinct o.subs_id) as free_7dt,
    sum(ch.total_subscribed) as purchase_from_7Dt_10D,
    sum(i.impressions) as impressions,
    sum(i.clicks) as clicks,
    ROUND(sum(i.spend_usd)::NUMERIC,2) as spend,
     ROUND((SUM(ch.total_subscribed) / COUNT(DISTINCT o.subs_id) * 100::NUMERIC), 2) AS subscription_rate,
    ROUND(SUM(customer_price)::NUMERIC, 2) AS gross_revenue,
    ROUND(SUM(customer_price)::NUMERIC / NULLIF(SUM(i.spend_usd)::NUMERIC, 0), 2) AS roas,
    ROUND(SUM(i.spend_usd)::NUMERIC / NULLIF(SUM(i.impressions)::NUMERIC, 0) * 1000, 2) AS cpm,
    ROUND(SUM(i.spend_usd)::NUMERIC / NULLIF(SUM(i.clicks)::NUMERIC, 0), 2) AS cpc,
    ROUND(SUM(i.spend_usd)::NUMERIC / NULLIF(SUM(ft.free_trial_count)::NUMERIC, 0), 2) AS cpt,
    ROUND(SUM(i.spend_usd)::NUMERIC / NULLIF(SUM(ch.total_subscribed)::NUMERIC, 0), 2) AS cpa
from offer_counts o
    left join charged_counts ch on o.subs_id = ch.subs_id
    and o.offer_event_date = ch.offer_event_date
    and o.country = ch.country
    left join (select * from investment_all_networks where platform = 'IOS') i on o.country = i.country and o.offer_event_date = i.date
    LEFT JOIN free_trial_counts ft ON o.offer_event_date = ft.offer_event_date AND o.country = ft.country
GROUP BY 1,
    2;
    