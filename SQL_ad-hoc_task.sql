/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Илларионов Андрей Андреевич
 * Дата: 24.11.2025 -- 
*/

/* Часть 1. Разработка витрины данных
 * Напишите ниже запрос для создания витрины данных
*/

WITH top_regions AS (
    -- Топ-3 региона по количеству заказов
    SELECT 
        u.region
    FROM ds_ecom.orders o
    JOIN ds_ecom.users u ON o.buyer_id = u.buyer_id
    WHERE o.order_status IN ('Доставлено', 'Отменено')
    GROUP BY u.region
    ORDER BY COUNT(o.order_id) DESC
    LIMIT 3
),
order_payments_money AS (
    -- Первый платеж по заказу: проверяем, был ли он денежным переводом
    SELECT
        order_id,
        CASE 
            WHEN payment_type = 'денежный перевод' THEN 1
            ELSE 0
        END AS used_money_transfer
    FROM (
        SELECT
            op.*,
            ROW_NUMBER() OVER (
                PARTITION BY op.order_id 
                ORDER BY op.payment_sequential
            ) AS rn
        FROM ds_ecom.order_payments op
    ) t
    WHERE rn = 1
),
order_payments_features AS (
    -- Информация о промокодах и рассрочке по всем платежам заказа
    SELECT 
        op.order_id,
        MAX(CASE WHEN op.payment_type = 'промокод' THEN 1 ELSE 0 END) AS is_promocod_used,
        MAX(CASE WHEN op.payment_installments > 1 THEN 1 ELSE 0 END) AS is_installment_used
    FROM ds_ecom.order_payments op
    GROUP BY op.order_id
),
order_costs AS (
    -- Стоимость заказов (только доставленные)
    SELECT 
        oi.order_id,
        SUM(oi.price + oi.delivery_cost) AS total_cost
    FROM ds_ecom.order_items oi
    JOIN ds_ecom.orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'Доставлено'
    GROUP BY oi.order_id
),
order_ratings AS (
    -- Рейтинги заказов с нормализацией шкалы
    SELECT 
        order_id,
        AVG(
            CASE 
                WHEN review_score BETWEEN 10 AND 50 THEN review_score / 10
                ELSE review_score
            END
        ) AS normalized_score
    FROM ds_ecom.order_reviews
    WHERE review_score IS NOT NULL
    GROUP BY order_id
),
user_orders_base AS (
    -- Базовая информация о заказах пользователей
    SELECT 
        u.user_id,
        u.region,
        o.order_id,
        o.order_purchase_ts,
        o.order_status,
        COALESCE(opf.is_promocod_used, 0)    AS is_promocod_used,
        COALESCE(opf.is_installment_used, 0) AS is_installment_used,
        COALESCE(opm.used_money_transfer, 0) AS used_money_transfer,
        COALESCE(oc.total_cost, 0)           AS order_cost,
        orr.normalized_score,
        CASE 
            WHEN o.order_status = 'Отменено' THEN 1 
            ELSE 0 
        END AS is_canceled
    FROM ds_ecom.users u
    JOIN ds_ecom.orders o 
        ON u.buyer_id = o.buyer_id
    JOIN order_payments_money opm 
        ON o.order_id = opm.order_id
    JOIN order_payments_features opf 
        ON o.order_id = opf.order_id
    LEFT JOIN order_costs oc 
        ON o.order_id = oc.order_id
    LEFT JOIN order_ratings orr 
        ON o.order_id = orr.order_id
    WHERE o.order_status IN ('Доставлено', 'Отменено')
      AND u.region IN (SELECT region FROM top_regions)
),
user_aggregated AS (
    -- Агрегация по пользователям и регионам
    SELECT 
        user_id,
        region,
        MIN(order_purchase_ts) AS first_order_ts,
        MAX(order_purchase_ts) AS last_order_ts,
        MAX(order_purchase_ts) - MIN(order_purchase_ts) AS lifetime,
        COUNT(*) AS total_orders,
        ROUND(AVG(normalized_score), 2) AS avg_order_rating,
        -- количество заказов, по которым есть оценка
        COUNT(normalized_score) AS num_orders_with_rating,
        -- количество отменённых заказов
        SUM(is_canceled) AS num_canceled_orders,
        -- доля отменённых заказов (ratio, без *100)
        ROUND(AVG(is_canceled), 4) AS canceled_orders_ratio,
        -- суммы и средние по стоимостям
        SUM(order_cost) AS total_order_costs,
        ROUND(
            AVG(order_cost) FILTER (WHERE order_status = 'Доставлено'),
            2
        ) AS avg_order_cost,
        -- рассрочка и промокоды
        SUM(is_installment_used) AS num_installment_orders,
        SUM(is_promocod_used) AS num_orders_with_promo,
        -- бинарные признаки
        MAX(used_money_transfer) AS used_money_transfer,
        MAX(is_installment_used) AS used_installments,
        MAX(is_canceled)         AS used_cancel
    FROM user_orders_base
    GROUP BY user_id, region
)
SELECT 
    user_id,
    region,
    first_order_ts,
    last_order_ts,
    lifetime,
    total_orders,
    avg_order_rating,
    num_orders_with_rating,
    num_canceled_orders,
    canceled_orders_ratio,
    total_order_costs,
    avg_order_cost,
    num_installment_orders,
    num_orders_with_promo,
    used_money_transfer,
    used_installments,
    used_cancel
FROM user_aggregated
ORDER BY total_orders DESC;


/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/
-- Напишите ваш запрос тут
WITH cte1 AS (
    SELECT 
        user_id,
        total_orders,
        total_order_costs,
        CASE 
            WHEN total_orders = 1 THEN 'Сегмент 1 заказа'
            WHEN total_orders BETWEEN 2 AND 5 THEN 'Сегмент 2-5 заказов'
            WHEN total_orders BETWEEN 6 AND 10 THEN 'Сегмент 6-10 заказов'
            ELSE 'Сегмент 11 и более заказов'
        END AS user_segmentation
    FROM ds_ecom.product_user_features
)
SELECT
    user_segmentation,
    COUNT(user_id) AS user_count,
    ROUND(AVG(total_orders)::numeric, 2) AS avg_orders,
    -- средняя стоимость ОДНОГО заказа в сегменте
    ROUND(SUM(total_order_costs)::numeric / SUM(total_orders), 2) AS avg_order_cost
FROM cte1
GROUP BY user_segmentation
ORDER BY avg_orders;

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * Большая часть пользователей действительно совершает всего один заказ — сегмент «1 заказ» формирует 
 * подавляющее большинство клиентской базы. Однако при корректном расчёте средней стоимости одного заказа 
 * становится заметно, что средний чек закономерно снижается по мере увеличения количества покупок. 
 * Это может указывать на то, что компания уже применяет некоторые механики стимулирования повторных покупок 
 * (например, скидки или акции), снижая итоговую стоимость заказа для лояльных пользователей.
Такой перекос в сторону «одиночных» покупок говорит о необходимости разработки стратегий удержания 
клиентов — программы лояльности, персональных предложений или стимулирования к повторным покупкам.
*/

/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/
-- Напишите ваш запрос тут
SELECT 
	user_id,
	total_orders,
	avg_order_cost 
FROM ds_ecom.product_user_features 
WHERE total_orders >= 3
ORDER BY avg_order_cost DESC 
LIMIT 15;

/* Напишите краткий комментарий с выводами по результатам задачи 2.
Пользователи с наибольшим средним чеком совершают относительно небольшое количество 
заказов — чаще всего 3–5. Несмотря на это, их средние траты значительно выше среднего по
 выборке, что делает их ценным премиальным сегментом для бизнеса.
Такие клиенты могут быть чувствительны к качеству обслуживания и персонализированным предложениям. 
Их поведение стоит анализировать отдельно и использовать для формирования таргетированных маркетинговых кампаний.
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/
-- Напишите ваш запрос тут
SELECT 
    region,
    COUNT(user_id) AS user_count,                       
    SUM(total_orders) AS orders_count,
    ROUND(SUM(total_order_costs)::numeric / NULLIF(SUM(total_orders), 0), 2) AS avg_order_cost,
    ROUND(SUM(num_installment_orders)::numeric * 100.0 / NULLIF(SUM(total_orders), 0), 2) AS part_installment_percent,
    ROUND(SUM(num_orders_with_promo)::numeric * 100.0 / NULLIF(SUM(total_orders), 0), 2) AS part_promo_percent,
    ROUND(AVG(used_cancel) * 100.0, 2) AS users_with_cancel_percent
FROM ds_ecom.product_user_features 
GROUP BY region;

/* Напишите краткий комментарий с выводами по результатам задачи 3.
Анализ показывает выраженную региональную специфику поведения пользователей.
Москва — крупнейший регион по числу клиентов и заказов. Здесь фиксируется высокий общий 
объём покупок, но вместе с тем и значительная доля пользователей, хотя бы раз отменявших заказ. 
Это может свидетельствовать о высокой нагрузке, разнообразии предложений или особенностях логистики.
Санкт-Петербург характеризуется более высоким средним чеком: пользователи чаще оформляют более 
дорогие заказы, хотя общее число клиентов меньше, чем в Москве.
Новосибирск демонстрирует более низкую долю отмен и относительно стабильное поведение покупателей. 
Это может указывать на более предсказуемую клиентскую базу и устойчивый спрос.
Такая региональная картина помогает формировать локальные стратегии: работать над снижением отмен в 
Москве, усиливать премиальные предложения в Петербурге и поддерживать стабильность в Новосибирске.
*/

/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/
-- Напишите ваш запрос тут

WITH cte1 AS (
    SELECT 
        TO_CHAR(first_order_ts, 'YYYY-Month') AS first_order_month,
        user_id,
        total_orders,
        total_order_costs,
        avg_order_rating,
        used_money_transfer,
        lifetime
    FROM ds_ecom.product_user_features 
    WHERE EXTRACT(YEAR FROM first_order_ts) = 2023
)
SELECT 
    first_order_month,
    COUNT(user_id) AS total_users, 
    SUM(total_orders) AS total_orders,
    ROUND(SUM(total_order_costs)::numeric / NULLIF(SUM(total_orders), 0), 2) AS avg_order_cost,
    ROUND(AVG(avg_order_rating)::numeric, 2) AS avg_order_rating,
    ROUND(AVG(used_money_transfer) * 100.0, 2) AS part_of_money_transfer_percent,
    AVG(lifetime) AS avg_lifetime
FROM cte1 
GROUP BY first_order_month
ORDER BY first_order_month;

/* Напишите краткий комментарий с выводами по результатам задачи 4.
Анализ распределения пользователей по первому месяцу заказа в 2023 году показывает, что активность 
клиентов заметно зависит от времени их появления. Пользователи, сделавшие первый заказ в начале года, 
демонстрируют более длительное время активности — что логично, поскольку у них было больше времени для 
совершения повторных покупок. Средний чек и рейтинг остаются относительно стабильными в течение года, 
хотя можно заметить умеренные колебания, которые потенциально могут быть связаны с сезонностью. 
Однако для подтверждения повторяемости таких факторов потребуется анализ данных нескольких лет.
Использование денежных переводов варьируется между месяцами, но для уверенных выводов также 
требуется более длинная временная серия. В целом структура поведения пользователей в течение года 
выглядит устойчивой, а различия больше связаны с временной доступностью для активности, чем с изменением качества аудитории.
*/




