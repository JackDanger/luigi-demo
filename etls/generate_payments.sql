owner: jackdanger
schedule: daily
data:
  number_of_rows: 100000
  production:
    tables:
      - name: cash_totals
      - name: card_totals
  development:
    tables:
      - name: jackdanger_cash_totals
      - name: jackdanger_card_totals
---

-- Generates 100,000 payments at random times during the day "{{{ now }}}"
{{#tables}}
DROP TABLE {{name}};
CREATE TABLE {{name}} (
  id           integer PRIMARY KEY,
  amount_cents integer NOT NULL,
  created_at   timestamp DEFAULT current_timestamp
);

INSERT INTO {{name}} (id, amount_cents, created_at)
  SELECT
    series.i,
    random() * 1000, -- cents
    (SELECT timestamp {{{ beginning_of_day }}} +
           ((timestamp {{{ end_of_day }}}
          - timestamp {{{ beginning_of_day }}}) * random()))
  FROM generate_series(1, {{{ number_of_rows }}}) as series(i);
{{/tables}}
