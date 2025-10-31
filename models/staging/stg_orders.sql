with source as (
    select * from {{ source('raw', 'olist_orders_dataset') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp as Purchased_date,
        order_approved_at as Confirmation_date,
        order_delivered_carrier_date as order_shipped_date,
        order_delivered_customer_date as order_delivered_date,
        order_estimated_delivery_date as estimated_delivery_date
    from source
)

select * from renamed