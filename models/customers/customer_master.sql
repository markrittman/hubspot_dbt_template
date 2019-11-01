{{
    config(
        materialized='table'
    )
}}
-- currently only BigQuery-compatible, because of CONCAT
-- Making it cross-database requires building custom adapter
-- (https://docs.getdbt.com/docs/building-a-new-adapter)



SELECT
    ROW_NUMBER() OVER() AS customer_id,
    CONCAT('Customer ', CAST(ROW_NUMBER() OVER() AS string)) AS demo_company_name,
    CASE WHEN harvest_customer_id IS NOT null THEN true ELSE false END AS is_services_client,
    CASE WHEN hubspot_company_id IS NOT null THEN true ELSE false END AS is_crm_tracked_client,
    CASE WHEN xero_is_supplier IS true THEN true ELSE false END AS is_supplier_company,
*
FROM
    (SELECT
        *
    FROM
        (SELECT
            customer_name,
            hubspot_company_id,
            xero_contact_id,
            harvest_customer_id,
            harvest_address,
            xero_is_customer,
            xero_is_supplier,
            xero_customer_status,
            harvest_customer_created_at,
            harvest_customer_currency,
            harvest_customer_is_active,
            hubspot_first_deal_created_date,
            hubspot_twitterhandle,
            hubspot_country,
            hubspot_total_money_raised,
            hubspot_city,
            hubspot_total_revenue,
            hubspot_annual_revenue,
            hubspot_website,
            hubspot_owner_id,
            hubspot_industry,
            hubspot_linkedin_bio,
            hubspot_is_public,
            hubspot_domain as web_domain,
            hubspot_created_date,
            hubspot_type,
            hubspot_state,
            hubspot_lifecycle_stage,
            hubspot_description,
            ROW_NUMBER() OVER (PARTITION BY LOWER(customer_name)) AS c_r
        FROM
            {{ ref('combined_raw_companies') }}
        {{ dbt_utils.group_by(n=29) }}
        ORDER BY
            1)
    WHERE
        c_r = 1)
union all
SELECT
  -999 AS customer_id,
  'Untracked Company' AS demo_company_name,
  FALSE AS is_services_client,
  false as is_crm_tracked_client,
  false as is_supplier_company,
  'Untracked Company' as customer_name,
  null as hubspot_company_id,
  null as xero_contact_id,
  null as harvest_customer_id,
  null as harvest_address,
  false as xero_is_customer,
  false as xero_is_supplier,
  null as xero_customer_status,
  null as harvest_customer_created_at,
  null as harvest_customer_currency,
  null as harvest_customer_is_active,
  null as hubspot_first_deal_created_date,
  null as hubspot_twitterhandle,
  null as hubspot_country,
  null as hubspot_total_money_raised,
  null as hubspot_city,
  null as hubspot_total_revenue,
  null as hubspot_annual_revenue,
  null as hubspot_website,
  null as hubspot_owner_id,
  null as hubspot_industry,
  null as hubspot_linkedin_bio,
  null as hubspot_is_public,
  null as web_domain,
  null as hubspot_created_date,
  null as hubspot_type,
  null as hubspot_state,
  null as hubspot_lifecycle_stage,
  null as hubspot_description,
  null as c_r
