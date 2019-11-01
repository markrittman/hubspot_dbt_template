{{
    config(
        materialized='table'
    )
}}
-- currently only BigQuery-compatible, because of UNNEST, OFFSET and CONCAT
-- Making it cross-database requires building custom adapter
-- (https://docs.getdbt.com/docs/building-a-new-adapter)
SELECT
    engagement_id AS communications_id,
    associations.companyids[OFFSET(OFF)] AS hubspot_company_id,
    associations.dealids AS sales_opportunity_id,
    engagement.timestamp AS communication_timestamp,
    engagement.ownerid AS communication_ownerid,
    CONCAT(CONCAT(o.firstname, ' '),o.lastname) AS owner_full_name,
    engagement.type AS communication_type,
    metadatato.email AS communications_to_email,
    metadata.text AS communications_text,
    metadata.subject AS communications_subject,
    CONCAT(CONCAT(metadata.from.firstname,' '), metadata.from.lastname) AS communications_from_firstname_lastname,
    metadata.status AS communications_status,
    metadatacc.email AS communications_cc_email
FROM
    {{ ref('hubspot_engagements') }},
    UNNEST(associations.companyids) WITH OFFSET OFF,
    UNNEST(associations.dealids),
    UNNEST(metadata.cc) metadatacc,
    UNNEST(metadata.to) metadatato
INNER JOIN
    {{ ref('hubspot_owners') }} o
    ON engagement.ownerid = o.ownerid
