{{
    config(
        materialized='table',
        description= 'Customer dimension with lifetime metrics and segmentation'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_payments as (
    select * from {{ ref('stg_order_payment') }}
),

-- Aggregate order metrics per customer
customer_orders as (
    select
        customer_id,
        count(distinct order_id) as lifetime_orders,
        min(purchased_date) as first_order_date,
        max(purchased_date) as most_recent_order_date,
        -- Calculate days between first and last order (date - date -> integer days)
      (max((purchased_date))::date - min((purchased_date)::date)) as customer_lifetime_days
    from orders
    where order_status not in ('canceled', 'unavailable')
    group by customer_id
),

-- Aggregate payment metrics per customer
customer_payments as (
    select
        o.customer_id,
        sum(op.payment_value) as lifetime_value,
        avg(op.payment_value) as avg_order_value,
        -- Count distinct payment types used
        count(distinct op.payment_type) as payment_methods
    from orders o
    left join order_payments op on o.order_id = op.order_id
    where o.order_status not in ('canceled', 'unavailable')
    group by o.customer_id
),

-- Join everything together
final as (
    select
        -- Customer attributes
        c.customer_id,
        c.customer_unique_id,
        c.zip_code,
        c.city,
        c.state,
        
        -- Order metrics
        coalesce(co.lifetime_orders, 0) as lifetime_orders,
        co.first_order_date,
        co.most_recent_order_date,
        co.customer_lifetime_days,
        
        -- Payment metrics
        coalesce(cp.lifetime_value, 0) as lifetime_value,
        cp.avg_order_value,
        cp.payment_methods,
        
        -- Segmentation fields
        case 
            when co.lifetime_orders >= 5 then 'High Frequency'
            when co.lifetime_orders >= 2 then 'Medium Frequency'
            when co.lifetime_orders = 1 then 'One-time'
            else 'No Orders'
        end as frequency_segment,
        
        case
            when coalesce(cp.lifetime_value,0) >= 1000 then 'High Value'
            when coalesce(cp.lifetime_value,0) >= 500 then 'Medium Value'
            when coalesce(cp.lifetime_value,0) > 0 then 'Low Value'
            else 'No Value'
        end as value_segment,
        
        -- Recency (days since last order) -- result is integer days (nullable)
        case 
            when co.most_recent_order_date is not null 
            then (current_date::date - co.most_recent_order_date::DATE)
            else null
        end as days_since_last_order,
        
        -- RFM composite segment (NULL-safe recency checks)
        case
            when co.lifetime_orders >= 3 
                 and coalesce(cp.lifetime_value,0) >= 500 
                 and co.most_recent_order_date is not null
                 and (current_date::date - co.most_recent_order_date::date) <= 180
            then 'Champion'
            
            when co.lifetime_orders >= 2 
                 and coalesce(cp.lifetime_value,0) >= 300
            then 'Loyal'
            
            when co.lifetime_orders = 1 
                 and coalesce(cp.lifetime_value,0) >= 200
            then 'Potential'
            
            when co.most_recent_order_date is not null
                 and (current_date::date - co.most_recent_order_date::date) > 365
            then 'At Risk'
            
            else 'Needs Attention'
        end as customer_segment

    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
    left join customer_payments cp on c.customer_id = cp.customer_id
)

select * from final
