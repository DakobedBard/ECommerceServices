#!/bin/bash
docker cp db/products.csv productDB:/products.csv
docker cp db/populate_products.sql productDB:/populate_products.sql
docker exec productDB psql -U postgres -d productdb -f populate_products.sql