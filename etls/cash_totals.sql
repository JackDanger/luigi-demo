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
  date         date NOT NULL,
  created_at   timestamp DEFAULT current_timestamp,
  PRIMARY KEY  (date)
);

INSERT INTO {{{ output_table }}} (total, date)
  SELECT COUNT(*) AS total, DATE(created_at) AS date
  FROM {{{ input_table }}}
  WHERE type = 'CashPayment' -- Demonstrating an inline comment
    AND DATE(created_at) = DATE({{{ now }}})
  GROUP BY DATE(created_at);
