owner: jackdanger
schedule: daily
requires:
  - generate_payments
data:
  production:
    output_table: cash_totals
    input_table: payments
  development:
    output_table: jackdanger_cash_totals
    input_table: jackdanger_payments
---
-- DROP TABLE IF EXISTS {{{ output_table }}};

CREATE TABLE IF NOT EXISTS {{{ output_table }}} (
  total        integer NOT NULL,
  day          date NOT NULL,
  created_at   timestamp DEFAULT current_timestamp,
  PRIMARY KEY  (day)
);

INSERT INTO {{{ output_table }}} (total, day)
  SELECT COUNT(*) AS total, DATE(created_at) AS day
  FROM {{{ input_table }}}
  WHERE type = 'CashPayment' -- Demonstrating an inline comment
    AND DATE(created_at) = DATE({{{ now }}})
  GROUP BY DATE(created_at);
