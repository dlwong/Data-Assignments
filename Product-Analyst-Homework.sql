-- connects to database
\c test; 
-- \dt shows tables
-- \d describes the table

CREATE TABLE table_purchases (
  user_id int,
  product VARCHAR(1),
  date_purchased date
);

CREATE TABLE table_prices (
  product VARCHAR(1),
  price float
);

CREATE TABLE table_wins (
  team VARCHAR(270),
  season varchar(4),
  games_won int
);

COPY table_purchases(user_id,product,date_purchased) from 'table_purchases.csv' DELIMITER ',' CSV;

-- to ignore the first row
-- COPY table_purchases(user_id,product,date_purchased) from 'table_purchases.csv' DELIMITER ',' CSV HEADER;

COPY table_prices(product,price) from 'table_prices.csv' DELIMITER ',' CSV;

COPY table_wins(team,season,games_won) from 'table_wins.csv' DELIMITER ',' CSV;


1. 
with b_buyers as (SELECT user_id
                   FROM TABLE_PURCHASES
                   WHERE product = 'B'),
  c_buyers as (
            SELECT user_id
              FROM TABLE_PURCHASES
              WHERE product = 'C'
  )
    SELECT COUNT(DISTINCT b.user_id) users
      FROM b_buyers b LEFT JOIN c_buyers c USING (user_id)
      WHERE c.user_id IS NULL;

2. 
SELECT COUNT(DISTINCT tp.user_id) users
  FROM TABLE_PURCHASES tp JOIN TABLE_PRICES tpp USING (product)
  WHERE price > 10 
    AND date_purchased >= '2015-05-01'
    AND date_purchased < '2015-06-01';

3.
WITH most_wins AS
  (SELECT team,
      max(games_won) max_games_won
  FROM TABLE_WINS
  GROUP BY 1) 
  SELECT mw.team, tw.season as season_with_most_wins
    FROM most_wins mw
      JOIN TABLE_WINS tw ON mw.team = tw.team 
      AND games_won = max_games_won;

4.
SELECT team, 
    season, 
    games_won,
    rank() over (partition by team order by games_won desc) season_rank
  from TABLE_WINS;
