SELECT
  p.label AS pipeline_label,
  _sdc_received_at,
  _sdc_batched_at,
  stages.stageid,
  stages.displayorder,
  stages.active,
  stages.label AS stage_label,
  stages.probability,
  stages.closedwon,
  pipelineid,
  p.displayorder AS pipeline_displayorder,
  p.active AS pipeline_active
FROM
  {{ ref('hubspot_deal_pipelines') }} p,
  UNNEST (stages) stages
