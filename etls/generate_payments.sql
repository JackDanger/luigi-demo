owner: jackdanger
schedule: hourly
data:
  payment_types:
    payment_type: CardPayment
    payment_type: CashPayment
  iterations: [1,2,4,8,16,32,64,128,256,512,1024,2048,4092,8192,16384]
  production:
    table: payments
  development:
    table: jackdanger_payments
---

-- DROP TABLE IF EXISTS {{{ table }}};
CREATE TABLE IF NOT EXISTS {{{ table }}} (
  amount_cents integer NOT NULL,
  type         VARCHAR(32) NOT NULL,
  created_at   timestamp DEFAULT current_timestamp
);

{{#payment_types}}

-- Generates a payment at a random time during the day identified as "{{{ now }}}"
INSERT INTO {{{ table }}} (amount_cents, type, created_at)
  SELECT
    random() * 1000, -- cents
    '{{{ payment_type }}}', -- this is the value of `payment_types` in this loop
    timestamp {{{ beginning_of_hour }}} + (
           (    timestamp {{{ end_of_hour }}}
              - timestamp {{{ beginning_of_hour }}}
            ) * random());

{{#iterations}}
-- Generates many more payments at random times.
-- The number of records that will be added is 2 to the power of the number of
-- values in `iterations`
INSERT INTO {{{ table }}} (amount_cents, type, created_at)
  SELECT
    random() * 1000,
    '{{{ payment_type }}}',
    timestamp {{{ beginning_of_hour }}} + (
           (    timestamp {{{ end_of_hour }}}
              - timestamp {{{ beginning_of_hour }}}
            ) * random())
  -- this doubles the dataset each query:
  FROM {{{ table }}}
  WHERE created_at BETWEEN {{{ beginning_of_hour }}} AND {{{ end_of_hour }}}
  AND type = '{{{ payment_type }}}';
{{/iterations}}
{{/payment_types}}
