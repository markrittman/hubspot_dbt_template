SELECT
  REPLACE(
    {{ remove_substrings('name', [', Ltd.', ', Inc', ' LLC', ' Cosmetics', ' Ltd.', ' Inc', ' Digital Limited', ' Group', ' Consulting']) }},
    'Resolving UK Limited',
    'Resolving Group Ltd'
  ) AS harvest_customer_name,
  id AS harvest_customer_id,
  address AS harvest_address,
  updated_at AS harvest_customer_updated_at,
  created_at AS harvest_customer_created_at,
  currency AS harvest_customer_currency,
  is_active AS harvest_customer_is_active
FROM
  {{ ref('harvest_base_clients') }}
