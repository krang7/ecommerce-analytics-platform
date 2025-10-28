with source as (
    select * from {{ source('raw', 'olist_orders_dataset') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        date(order_purchase_timestamp) as Purchased_date,
        date(order_approved_at) as Confirmation_date,
        date(order_delivered_carrier_date) as order_shipped_date,
        date(order_delivered_customer_date) as order_delivered_date,
        date(order_estimated_delivery_date) as estimated_delivery_date
    from source
)

select * from renamed