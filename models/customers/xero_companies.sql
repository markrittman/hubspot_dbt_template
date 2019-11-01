SELECT
    contactid AS xero_contact_id,
    REPLACE(
        {{ remove_substrings('name', [', Ltd.', ', Inc', ' LLC', ' Cosmetics', ' Ltd.', ' Inc', ' Digital Limited', ' Group', ' Consulting', ' Computing.']) }},
        'BHCC',
        'Brighton & Hove City Council'
    ) AS xero_customer_name,
    iscustomer AS xero_is_customer,
    issupplier AS xero_is_supplier,
    defaultcurrency AS xero_customer_default_currency,
    contactstatus AS xero_customer_status,
    contactnumber AS xero_customer_contact_number
FROM
    {{ ref('xero_base_contacts') }}
WHERE
  firstname IS null
{{ dbt_utils.group_by(n=7) }}
