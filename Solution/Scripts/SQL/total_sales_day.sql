SELECT date, SUM(total_sales) AS total_sales
FROM transactions
GROUP BY date
ORDER BY date;
