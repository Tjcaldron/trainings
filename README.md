CREATE VIEW vw_top_shippers_by_weight AS
SELECT 
    shipper_party_name,
    country_name,
    SUM(harmonized_weight) AS total_weight,
    harmonized_weight_unit,
    COUNT(DISTINCT identifier) AS total_shipments
FROM shipper_gold_2019
GROUP BY shipper_party_name, country_name, harmonized_weight_unit
ORDER BY total_weight DESC
