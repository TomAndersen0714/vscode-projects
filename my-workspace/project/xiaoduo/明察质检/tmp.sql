ch_test_data_sync_1_msg_header = PulsarHook.get_ch_msg_header(
    target_table="buffer.test_data_sync_1_buffer",
    source_table="numbers(100)",
    clear_table="test.data_sync_1_local",
    partition="{{ds_nodash}}",
    cluster_name=CLUSTER_NAME
)