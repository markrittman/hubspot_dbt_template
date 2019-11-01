SELECT
  d.* EXCEPT (dealstage,
    pipeline),
  p.stage_label AS dealstage,
  p.pipeline_label AS pipeline,
  p.displayorder AS dealstage_sortindex,
  concat(concat(u.first_name,' '),u.last_name) as salesperson_full_name
FROM
  {{ ref('crm_deals') }} d
JOIN
  {{ ref('crm_deal_pipelines') }} p
ON
  d.dealstage = p.stageid
LEFT OUTER JOIN {{ ref('users') }} u
on d.salesperson_email = u.email
