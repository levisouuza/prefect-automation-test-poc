echo "Iniciando configurações"

echo "Ativando profile local"
prefect profile use local

echo "Configurando API URL"
prefect config set PREFECT_API_URL='http://127.0.0.1:4200/api'




