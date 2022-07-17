#!/bin/bash

echo "2022.01.01-2022.01.13.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.01-2022.01.13.snappy.parquet
sleep 3s

echo "2022.01.14.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.14.snappy.parquet
sleep 3s

echo "2022.01.15-01.20.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.15-01.20.snappy.parquet
sleep 3s

echo "2022.01.20-01.25.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.20-01.25.snappy.parquet
sleep 3s

echo "2022.01.25-01_31.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.25-01_31.snappy.parquet
sleep 3s

echo "2022.01.31.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.31.snappy.parquet
sleep 3s

echo "2022.02.01-02.20.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.02.01-02.20.snappy.parquet
sleep 3s

echo "2022.02.21-02.31.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.02.21-02.31.snappy.parquet
sleep 3s

echo "2022.03.01-03.15.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.03.01-03.15.snappy.parquet
sleep 3s

echo "2022.03.16-03.31.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.03.16-03.31.snappy.parquet
sleep 3s

echo "2022.04.01-04.15.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.04.01-04.15.snappy.parquet
sleep 3s

echo "2022.04.16-04.28.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.04.16-04.28.snappy.parquet
sleep 3s

echo "2022.04.29-05.13.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.04.29-05.13.snappy.parquet
sleep 3s

echo "2022.05.14-05.30.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.05.14-05.30.snappy.parquet
sleep 3s

echo "2022.05.31-06.16.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.05.31-06.16.snappy.parquet
sleep 3s

echo "2022.06.17-06.29.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.06.17-06.29.snappy.parquet
sleep 3s

echo "2022.06.30-2022.07.07.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.06.30-2022.07.07.snappy.parquet
sleep 3s

echo "2022_07_08.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022_07_08.snappy.parquet
sleep 3s

echo "2022_07_09-12.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022_07_09-12.snappy.parquet
sleep 3s

echo "2022_07_13.snappy.parquet" 
docker exec -i 0c93252f8f95 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022_07_13.snappy.parquet
sleep 3s
