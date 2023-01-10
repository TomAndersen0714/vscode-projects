uc-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlaWQiOiJxLTYyOTQ2OTJlODZlY2NlYTQzNDNhNDM3YyIsInVpZCI6IjYyOTQ2OTVlODcwNjk0OWY0Zjk3YWE1MSIsInR5cGUiOjEsInN1cGVyIjp0cnVlLCJleHAiOjE2NzMxNjM2NDZ9.YTFxNndbQ6tRAfDGaI99ey9HhIiBWr33be_dYM176qA



eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoi5pmT5aSaIiwidG9rZW4iOiJ4aWFvZHVvYWkiLCJleHAiOjE2NzMxNjA5MzAsImlzcyI6InhpYW9kdW9haSJ9.nV6SWot4juwHMXNRzIbkYO3jL16wqW6B6gQcaKCwX30


Xd-Pub-Token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoi5pmT5aSaIiwidG9rZW4iOiJ4aWFvZHVvYWkiLCJleHAiOjE2NzMxNjA5MzAsImlzcyI6InhpYW9kdW9haSJ9.nV6SWot4juwHMXNRzIbkYO3jL16wqW6B6gQcaKCwX30

Cookie: uc-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlaWQiOiJxLTYyOTQ2OTJlODZlY2NlYTQzNDNhNDM3YyIsInVpZCI6IjYyOTQ2OTVlODcwNjk0OWY0Zjk3YWE1MSIsInR5cGUiOjEsInN1cGVyIjp0cnVlLCJleHAiOjE2NzMxNjM2NDZ9.YTFxNndbQ6tRAfDGaI99ey9HhIiBWr33be_dYM176qA



CREATE TABLE app_ciae.data_index_all (
    `business_category` String,
    `data_idx_obj_name` String,
    `data_idx_obj_value` String,
    `data_idx_name` String,
    `data_idx_value` Float64,
    `create_time` String,
    `time_type` String,
    `data_source` String,
    `shop_id` String,
    `platform` String,
    `day` Int32
) ENGINE = Distributed(
    'cluster_3s_2r',
    'app_ciae',
    'data_index_local',
    rand()
)
