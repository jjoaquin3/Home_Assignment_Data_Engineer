SELECT product, SUM(total_sales) AS total_sales
FROM transactions
GROUP BY product
ORDER BY total_sales DESC;
