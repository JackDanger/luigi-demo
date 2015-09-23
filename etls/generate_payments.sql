owner: jackdanger
schedule: daily
data:
  number_of_rows: 100000
  payment_types:
    - CardPayment
    - CardPayment
  production:
    table: payments
  development:
    table: jackdanger_payments
---

-- Generates 100,000 payments at random times during the day "{{{ now }}}"
DROP TABLE {{table}};
CREATE TABLE {{table}} (
  id           SERIAL,
  amount_cents integer NOT NULL,
  type         VARCHAR(32) NOT NULL,
  created_at   timestamp DEFAULT current_timestamp
);

{{#payment_types}}
INSERT INTO {{table}} (amount_cents, type, created_at)
  SELECT
    random() * 1000, -- cents
    '{{{.}}}', -- this is the value of `payment_types` in this loop
    times.t
  FROM (
    SELECT timestamp {{{ beginning_of_day }}} +
         (
           (
                timestamp {{{ end_of_day }}}
              - timestamp {{{ beginning_of_day }}}
            ) * random()
          ) t
    FROM generate_series(1, {{{ number_of_rows }}})
  ) as times;
{{/payment_types}}
