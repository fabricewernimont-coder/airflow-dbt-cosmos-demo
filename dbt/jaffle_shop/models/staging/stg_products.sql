with source as (
    select * from {{ ref('raw_products') }} -- On pointe sur le vrai nom du fichier
),

renamed as (
    select
        product_id,
        description as product_name,
        category as product_category
    from source
)

select * from renamed