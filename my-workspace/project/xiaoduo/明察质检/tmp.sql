6decd0a1801d    data_receiver_jd_order_all_zhike_only
e80f15b5be0f    data_receiver_chat_event_zhike_only
58b2c94358f4    data_receiver_history_order_event_zhike_only
fef57ef75373    data_receiver_complain_tags_zhike_only


docker stop 6decd0a1801d
docker stop e80f15b5be0f
docker stop 58b2c94358f4
docker stop fef57ef75373


docker start 6decd0a1801d
docker start e80f15b5be0f
docker start 58b2c94358f4
docker start fef57ef75373

