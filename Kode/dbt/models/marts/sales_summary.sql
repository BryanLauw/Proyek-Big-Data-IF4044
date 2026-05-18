{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT
    r.r_name                                                          AS region_name,
    n.n_name                                                          AS nation_name,
    o.o_orderdate,
    o.o_orderstatus,
    SUM({{ calculate_revenue('l.l_extendedprice', 'l.l_discount') }}) AS total_revenue,
    SUM(l.l_quantity)                                                 AS total_quantity,
    COUNT(DISTINCT o.o_orderkey)                                      AS order_count
FROM {{ source('tpch', 'orders') }}   o
JOIN {{ source('tpch', 'lineitem') }} l ON o.o_orderkey  = l.l_orderkey
JOIN {{ source('tpch', 'customer') }} c ON o.o_custkey   = c.c_custkey
JOIN {{ source('tpch', 'nation') }}   n ON c.c_nationkey = n.n_nationkey
JOIN {{ source('tpch', 'region') }}   r ON n.n_regionkey = r.r_regionkey
GROUP BY
    r.r_name,
    n.n_name,
    o.o_orderdate,
    o.o_orderstatus
