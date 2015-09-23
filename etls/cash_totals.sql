owner: jackdanger
schedule: daily
requires:
  - generate_payments
data:
  production:
    output_table: cash_totals
  development:
    output_table: jackdanger_cash_totals
---
INSERT INTO {{{ output_table }}} (total, date)
  SELECT COUNT(*) AS total, DATE(created_at) AS date
  FROM payments
  WHERE type = 'CashPayment' -- Demonstrating an inline comment
    AND DATE(created_at) == {{{ now }}}
  GROUP BY DATE(created_at)
