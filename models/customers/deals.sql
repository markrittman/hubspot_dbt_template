{{
    config(
        materialized='table'
    )
}}
-- currently only BigQuery-compatible, because of UNNEST and OFFSET
-- Making it cross-database requires building custom adapter
-- (https://docs.getdbt.com/docs/building-a-new-adapter)
SELECT
    *
FROM
    (
      SELECT
          associations.associatedcompanyids[OFFSET(OFF)] AS associatedcompanyids,
          properties.hs_sales_email_last_replied.value AS sales_email_last_replied,
          properties.closed_lost_reason.value AS closed_lost_reason,
          properties.dealname.value AS dealname,
          properties.hubspot_owner_id.value AS hubspot_owner_id,
          replace(properties.hubspot_owner_id.sourceid,'mjr-analytics.com','rittmananalytics.com') as hubspot_owner_email,
          properties.hs_lastmodifieddate.value AS lastmodifieddate,
          properties.notes_last_updated.value AS notes_last_updated,
          CASE WHEN properties.dealstage.value = '7a1f6388-75b5-479c-99cb-da3479c12629' THEN 'Sales Opportunity Identified'
               WHEN properties.dealstage.value = '553a886b-24bc-4ec4-bca3-b1b7fcd9e1c7' THEN 'Deal Verbally Closed subject to SoW + MSA'
               WHEN properties.dealstage.value = '7c41062e-06c6-4a4a-87eb-de503061b23c' THEN 'Sales Closed Won and Delivered'
               WHEN properties.dealstage.value = 'presentationscheduled' THEN 'Proposal Sent'
               WHEN properties.dealstage.value = 'appointmentscheduled' THEN 'Initial Inbound Enquiry'
               WHEN properties.dealstage.value = 'qualifiedtobuy' THEN 'Initial Meeting & Presentation'
               WHEN properties.dealstage.value = 'contractsent' THEN 'SoW and MSA Sent or awaiting'
               WHEN properties.dealstage.value = 'closedwon' THEN 'Closed Won and Awaiting Delivery'
               WHEN properties.dealstage.value = 'closedlost' THEN 'Sales Closed Lost'
               ELSE properties.dealstage.value
               END AS dealstage,
         CASE WHEN properties.dealstage.value = '7a1f6388-75b5-479c-99cb-da3479c12629' THEN 0
               WHEN properties.dealstage.value = '553a886b-24bc-4ec4-bca3-b1b7fcd9e1c7' THEN 4
               WHEN properties.dealstage.value = '7c41062e-06c6-4a4a-87eb-de503061b23c' THEN 8
               WHEN properties.dealstage.value = 'presentationscheduled' THEN 3
               WHEN properties.dealstage.value = 'appointmentscheduled' THEN 1
               WHEN properties.dealstage.value = 'qualifiedtobuy' THEN 2
               WHEN properties.dealstage.value = 'contractsent' THEN 5
               WHEN properties.dealstage.value = 'closedwon' THEN 6
               WHEN properties.dealstage.value = 'closedlost' THEN 7
               END AS dealstage_sortindex,
          CASE WHEN properties.dealstage.value = '7a1f6388-75b5-479c-99cb-da3479c12629' THEN 10
               WHEN properties.dealstage.value = '553a886b-24bc-4ec4-bca3-b1b7fcd9e1c7' THEN 80
               WHEN properties.dealstage.value = '7c41062e-06c6-4a4a-87eb-de503061b23c' THEN 100
               WHEN properties.dealstage.value = 'presentationscheduled' THEN 40
               WHEN properties.dealstage.value = 'appointmentscheduled' THEN 20
               WHEN properties.dealstage.value = 'qualifiedtobuy' THEN 40
               WHEN properties.dealstage.value = 'contractsent' THEN 90
               WHEN properties.dealstage.value = 'closedwon' THEN 100
               WHEN properties.dealstage.value = 'closedlost' THEN 0
               END AS dealstage_pipeline_modifier,
          properties.dealstage.value as deadstage_id,
          properties.pipeline.value AS pipeline,
          properties.dealtype.value AS dealtype,
          properties.closedate.value AS closedate,
          properties.createdate.value AS createdate,
          properties.amount.value AS amount,
          properties.notes_last_contacted.value AS notes_last_contacted,
          properties.amount_in_home_currency.value AS amount_in_home_currency,
          properties.hubspot_owner_assigneddate.value AS hubspot_owner_assigneddate,
          properties.num_notes.value AS num_notes,
          properties.description.value AS description,
          dealid AS deal_id,
          _sdc_sequence,
          MAX(_sdc_sequence) OVER (PARTITION BY dealid ORDER BY _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_sdc_sequence
      FROM
          {{ ref('hubspot_deals') }},
          UNNEST(associations.associatedcompanyids) WITH OFFSET OFF
    )
WHERE
    _sdc_sequence = latest_sdc_sequence
