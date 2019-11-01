{{
    config(
        materialized='table',
        partition_by='DATE(event_ts)'
    )
}}
SELECT
    *,
    {{ dbt_utils.datediff('last_billable_day_ts', current_timestamp(), 'day')}} AS days_since_last_billable_day,
    {{ dbt_utils.datediff('last_incoming_email_ts', current_timestamp(), 'day')}} AS days_since_last_incoming_email,
    {{ dbt_utils.datediff('last_outgoing_email_ts', current_timestamp(), 'day')}} AS days_since_last_outgoing_email
FROM
  (SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts) AS event_seq,
      MIN(CASE WHEN event_type = 'Billable Day' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS first_billable_day_ts,
      MAX(CASE WHEN event_type = 'Billable Day' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_billable_day_ts,
      MIN(CASE WHEN event_type = 'Client Invoiced' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS first_invoice_day_ts,
      MAX(CASE WHEN event_type = 'Client Invoiced' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_invoice_day_ts,
      MAX(CASE WHEN event_type = 'Incoming Email' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_incoming_email_ts,
      MAX(CASE WHEN event_type = 'Outgoing Email' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_outgoing_email_ts,
      MIN(event_ts)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS first_contact_ts,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type = 'Billable Day' THEN date(event_ts) END)          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},MONTH) AS months_since_first_billable_day,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type = 'Billable Day' THEN date(event_ts) END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},WEEK) AS weeks_since_first_billable_day,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type like '%Email%' THEN date(event_ts) END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},MONTH) AS months_since_first_contact_day,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type like '%Email%' THEN date(event_ts) END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},WEEK) AS weeks_since_first_contact_day,
      MAX(CASE WHEN event_type = 'Billable Day' THEN true ELSE false END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS billable_client,
      MAX(CASE WHEN event_type LIKE '%Sales%' THEN true ELSE false END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS sales_prospect
  FROM
  -- sales opportunity stages
      (SELECT
  		    deals.lastmodifieddate AS event_ts,
          customer_master.customer_id AS customer_id,
          customer_master.customer_name AS customer_name,
          concat(concat(owners.firstname,' '),owners.lastname) as event_source,
  	      deals.dealname AS event_details,
  	      deals.dealstage AS event_type,
  	      AVG(deals.amount) AS event_value,
          sum(1) as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('deals') }} AS deals
          ON customer_master.hubspot_company_id = deals.associatedcompanyids
      LEFT JOIN
          {{ ref('hubspot_owners') }} AS owners
          ON deals.hubspot_owner_id = CAST(owners.ownerid AS STRING)
      WHERE
          deals.lastmodifieddate IS NOT null
      {{ dbt_utils.group_by(n=6) }}
      UNION ALL
  -- consulting days
      SELECT
        	time_entries.spent_date AS event_ts,
        	customer_master.customer_id AS customer_id,
          customer_master.customer_name AS customer_name,
          time_entries.consultant_firstname_lastname as event_source,
  	      projects.name AS event_details,
  	      CASE WHEN time_entries.billable THEN 'Billable Day' ELSE 'Non-Billable Day' END AS event_type,
  	      time_entries.billable_rate*8 AS event_value,
          time_entries.hours/8 as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('harvest_projects') }} AS projects
          ON customer_master.harvest_customer_id = projects.client_id
      LEFT JOIN
          {{ ref('harvest_time_entries') }} AS time_entries
          ON time_entries.project_id = projects.id
      WHERE
          time_entries.spent_date IS NOT null
      {{ dbt_utils.group_by(n=8) }}
  UNION ALL
  -- incoming and outgoing emails
      SELECT
        	communications.communication_timestamp AS event_ts,
        	customer_master.customer_id AS customer_id,
          customer_master.customer_name AS customer_name,
          communications.communications_from_firstname_lastname as event_source,
          communications.communications_subject AS event_details,
          CASE WHEN communications.communication_type = 'INCOMING_EMAIL' THEN 'Incoming Email'
               WHEN communications.communication_type = 'EMAIL' THEN 'Outgoing Email'
               ELSE communications.communications_subject
          END AS event_type,
          1 AS event_value,
          1 as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('communications') }} AS communications
          ON customer_master.hubspot_company_id = communications.hubspot_company_id
      WHERE
          communications.communication_timestamp IS NOT null
      {{ dbt_utils.group_by(n=8) }}
  UNION ALL
      SELECT
        	invoices.issue_date AS event_ts,
        	customer_master.customer_id AS customer_id,
          customer_master.customer_name AS customer_name,
          cast(null as string) as event_source,
  	      invoices.subject AS event_details,
          'Client Invoiced' AS event_type,
  	      SUM(invoices.amount) AS event_value,
          sum(1) as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('harvest_invoices') }} AS invoices
          ON customer_master.harvest_customer_id = invoices.client_id
      WHERE
          invoices.issue_date IS NOT null
      {{ dbt_utils.group_by(n=6) }}
  UNION ALL
      SELECT
  	     invoices.issue_date AS event_ts,
  	     customer_master.customer_id AS customer_id,
         customer_master.customer_name AS customer_name,
         cast(null as string) as event_source,
         invoice_line_items.description AS event_details,
         'Client Credited' AS event_type,
  	      COALESCE(SUM(invoice_line_items.amount ), 0) AS event_value,
          sum(1) as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('harvest_invoices') }} AS invoices
          ON customer_master.harvest_customer_id = invoices.client_id
      LEFT JOIN
          {{ ref('harvest_invoice_line_items') }} AS invoice_line_items
          ON invoices.id = invoice_line_items.invoice_id
      {{ dbt_utils.group_by(n=6) }}
      HAVING
  	     (COALESCE(SUM(invoice_line_items.amount ), 0) < 0)
  UNION ALL
      SELECT
              pageviews.event_ts AS event_ts,
              customer_master.customer_id  AS customer_id,
              customer_master.customer_name AS customer_name,
              pageviews.visitor_city as event_source,
              pageviews.page_title AS event_details,
              concat(pageviews.site,' site visit') AS event_type,
              sum(1) as event_value,
              sum(1) as event_units
      FROM
          {{ ref('customer_master') }}  AS customer_master
     LEFT JOIN
          {{ ref('pageviews') }} AS pageviews
          ON customer_master.customer_id = pageviews.customer_id
      {{ dbt_utils.group_by(n=6) }}
  UNION ALL
      SELECT
      timestamp_trunc(History_Completed_Time,DAY) AS event_ts,
      c.customer_id AS customer_id,
      c.customer_name AS customer_name,
      all_history.User_Name as event_source,
      all_history.dashboard_title as event_details,
      'daily_looker_usage_mins' AS event_type,
      SUM(all_history.History_Approximate_Web_Usage_in_Minutes )/60 AS event_value,
      sum(1) as event_units
    FROM
      {{ ref('all_history') }} AS all_history
    JOIN
      {{ ref('customer_master') }} AS c
    ON
      all_history.site = c.customer_name
    {{ dbt_utils.group_by(n=6) }}
  UNION ALL
          SELECT
                  event_ts,
                  customer_master.customer_id  AS customer_id,
                  customer_master.customer_name AS customer_name,
                  client_slack_messages.communications_from_firstname_lastname as event_source,
                  client_slack_messages.communications_text AS event_details,
                  'client_slack_message' AS event_type,
                  sum(1) as event_value,
                  sum(1) as event_units
          FROM
              {{ ref('customer_master') }}  AS customer_master
         LEFT JOIN
              {{ ref('client_slack_messages') }} AS client_slack_messages
              ON customer_master.customer_id = client_slack_messages.customer_id
          {{ dbt_utils.group_by(n=6) }}
  UNION ALL
          SELECT
                  opportunity_dealstage_events.event_ts,
                  customer_master.customer_id  AS customer_id,
                  customer_master.customer_name AS customer_name,
                  cast(null as string) as event_source,
                  opportunity_dealstage_events.notes AS event_details,
                  opportunity_dealstage_events.opportunity_stage as event_type,
                  sum(cast(opportunity_dealstage_events.opportunity_value as int64)) as event_value,
                  sum(1) as event_units
          FROM
              {{ ref('customer_master') }}  AS customer_master
         LEFT JOIN
              {{ ref('opportunity_dealstage_events') }} AS opportunity_dealstage_events
              ON customer_master.customer_id = opportunity_dealstage_events.customer_id
          {{ dbt_utils.group_by(n=6) }}
  UNION ALL
  SELECT
  created AS event_ts,
  customer_master.customer_id AS customer_id,
  customer_master.customer_name AS customer_name,
  stories.displayname as event_source,
  summary AS event_details,
  'Jira User Story Created' AS event_type,
  1 AS event_value,
  1 as event_units
FROM
  {{ ref('customer_master') }} AS customer_master
LEFT JOIN
  `ra-development.stitch_jira.project_mapping` AS mapping
ON
  customer_master.customer_name = mapping.customer_name
LEFT JOIN
  {{ ref('dev_projects') }} AS projects
ON
  mapping.string_field_0 = projects.name
LEFT JOIN
  {{ ref('dev_stories') }} AS stories
ON
  projects.id = stories.project_id
WHERE
  created IS NOT NULL
  AND ifnull(issuetype_name,
    'Story') = 'Story'
UNION ALL
SELECT
  updated AS event_ts,
  customer_master.customer_id AS customer_id,
  customer_master.customer_name AS customer_name,
  stories.displayname as event_source,
  summary AS event_details,
  'Jira User Story Closed' AS event_type,
  1 AS event_value,
  1 as event_units
FROM
  {{ ref('customer_master') }} AS customer_master
LEFT JOIN
  `ra-development.stitch_jira.project_mapping` AS mapping
ON
  customer_master.customer_name = mapping.customer_name
LEFT JOIN
  {{ ref('dev_projects') }} AS projects
ON
  mapping.string_field_0 = projects.name
LEFT JOIN
  {{ ref('dev_stories') }} AS stories
ON
  projects.id = stories.project_id
WHERE
  updated IS NOT NULL
  AND ifnull(issuetype_name,
    'Story') = 'Story'
  AND statuscategory = 'Done'
UNION ALL
SELECT
  created AS event_ts,
  customer_master.customer_id AS customer_id,
  customer_master.customer_name AS customer_name,
  stories.displayname as event_source,
  summary AS event_details,
  'Jira Task Created' AS event_type,
  1 AS event_value,
  1 as event_units
FROM
  {{ ref('customer_master') }} AS customer_master
LEFT JOIN
  `ra-development.stitch_jira.project_mapping` AS mapping
ON
  customer_master.customer_name = mapping.customer_name
LEFT JOIN
  {{ ref('dev_projects') }} AS projects
ON
  mapping.string_field_0 = projects.name
LEFT JOIN
  {{ ref('dev_stories') }} AS stories
ON
  projects.id = stories.project_id
WHERE
  updated IS NOT NULL
  AND ifnull(issuetype_name,
    'Story') = 'Task'
UNION ALL
SELECT
  updated AS event_ts,
  customer_master.customer_id AS customer_id,
  customer_master.customer_name AS customer_name,
  stories.displayname as event_source,
  summary AS event_details,
  'Jira Task Closed' AS event_type,
  1 AS event_value,
  1 as event_units
FROM
  {{ ref('customer_master') }} AS customer_master
LEFT JOIN
  `ra-development.stitch_jira.project_mapping` AS mapping
ON
  customer_master.customer_name = mapping.customer_name
LEFT JOIN
  {{ ref('dev_projects') }} AS projects
ON
  mapping.string_field_0 = projects.name
LEFT JOIN
  {{ ref('dev_stories') }} AS stories
ON
  projects.id = stories.project_id
WHERE
  updated IS NOT NULL
  AND ifnull(issuetype_name,
    'Story') = 'Task'
  AND statuscategory = 'Done'
  UNION ALL
      SELECT
          *
      FROM
          (SELECT
              invoices.paid_at AS event_ts,
  	      customer_master.customer_id AS customer_id,
              customer_master.customer_name AS customer_name,
              cast(null as string) as event_source,
              invoices.subject AS event_details,
              CASE WHEN invoices.paid_at <= invoices.due_date THEN 'Client Paid' ELSE 'Client Paid Late' END AS event_type,
  	          SUM(invoices.amount) AS event_value,
              sum(1) as event_units
          FROM
              {{ ref('customer_master') }} AS customer_master
          LEFT JOIN {{ ref('harvest_invoices') }}  AS invoices
              ON customer_master.harvest_customer_id = invoices.client_id
          WHERE
            invoices.paid_at IS NOT null
          {{ dbt_utils.group_by(n=6) }}
          )
      )
  WHERE
      customer_name NOT IN ('Rittman Analytics', 'MJR Analytics')
  )
