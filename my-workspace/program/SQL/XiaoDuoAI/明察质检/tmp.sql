#!/bin/bash
echo "2022.01.01-2022.01.13.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.01-2022.01.13.snappy.parquet
echo "2022.01.14.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.14.snappy.parquet
echo "2022.01.15-01.20.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.15-01.20.snappy.parquet
echo "2022.01.20-01.25.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.20-01.25.snappy.parquet
echo "2022.01.25-01.31.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.25-01.31.snappy.parquet
echo "2022.01.31.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.01.31.snappy.parquet
echo "2022.02.01-02.20.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.02.01-02.20.snappy.parquet
echo "2022.02.21-02.31.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.02.21-02.31.snappy.parquet
echo "2022.03.01-2022.03.15.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.03.01-2022.03.15.snappy.parquet
echo "2022.03.16-03.31.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.03.16-03.31.snappy.parquet
echo "2022.04.01-04.15.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.04.01-04.15.snappy.parquet
echo "2022.04.16-04.28.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.04.16-04.28.snappy.parquet
echo "2022.04.29-05.13.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.04.29-05.13.snappy.parquet
echo "2022.05.14-05.30.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.05.14-05.30.snappy.parquet
echo "2022.05.31-06.16.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.05.31-06.16.snappy.parquet
echo "2022.06.17-06.29.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.06.17-06.29.snappy.parquet
echo "2022.06.30-07.07.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.06.30-07.07.snappy.parquet
echo "2022.07.08.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.07.08.snappy.parquet
echo "2022.07.09-07.12.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.07.09-07.12.snappy.parquet
echo "2022.07.13.snappy.parquet" 
docker exec -i 42198f0fe342 clickhouse-client --port=29000 --query="INSERT INTO buffer.sxx_tmp_outbound_workorder_buffer FORMAT Parquet" < 2022.07.13.snappy.parquet