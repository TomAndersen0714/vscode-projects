SELECT productID,
    bitAnd(
        sumDistinctIf(orgQuantity, orgQuantity >= 4294967296),
        4294967295
    ) + sumIf(orgQuantity, orgQuantity < 4294967296) AS totalOrgQuantity
FROM f_order_3
GROUP BY productID