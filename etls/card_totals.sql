owner: jackdanger
schedule: daily
requires:
  - generate_payments
data:
  sleep: 1
  production:
    output_table: card_totals
    input_table: payments
  development:
    output_table: jackdanger_card_totals
    input_table: jackdanger_payments
---
DROP TABLE IF EXISTS {{{ output_table }}};

CREATE TABLE  {{{ output_table }}} (
  id           SERIAL,
  total        integer NOT NULL,
  date         date NOT NULL,
  created_at   timestamp DEFAULT current_timestamp
);

INSERT INTO {{{ output_table }}} (total, date)
  SELECT COUNT(*) AS total, DATE(created_at) AS date
  FROM {{{ input_table }}}
  WHERE type = 'CardPayment' -- Demonstrating an inline comment
    AND DATE(created_at) = DATE({{{ now }}})
  GROUP BY DATE(created_at);

SELECT pg_sleep({{{sleep}}});

