#!/bin/bash

echo "- Suppression du conteneur DuckDB si il existe..."
docker container rm -f duckdb

echo "- Lancement de DuckDB via Docker..."
docker-compose up 
