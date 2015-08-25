owner: jackdanger
schedule: daily
requires:
  - card_totals
  - cash_totals
data:
  production:
    output_table: payment_totals
    payment_tables:
      - cash_totals
      - card_totals
  development:
    output_table: jackdanger_payment_totals
    payment_tables:
      - jackdanger_cash_totals
      - jackdanger_card_totals
---
{{#payment_tables}}
INSERT INTO {{{output_table}}} (total, type, day)
  SELECT total, '{{.}}', day
  FROM {{.}};
{{/payment_tables}}


