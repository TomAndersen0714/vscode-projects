func (u *MiNiDialogManager) CreateSamples(startDay, endDay, platform string, groupList []string, shopList []string, qidList []int64, bucketSize uint, filterRule bool) (string, error) {
	//startDay = fmt.Sprintf("%s-%s-%s", startDay[0:4], startDay[4:6], startDay[6:8])
	//endDay = fmt.Sprintf("%s-%s-%s", endDay[0:4], endDay[4:6], endDay[6:8])
	sql := `WITH t1 AS
  (SELECT split_part(snick, ':', 1) AS seller_nick,
          cnick,
          DAY,
          create_time,
          uuid() AS sample_id
   FROM dwd.mini_xdrs_log
   WHERE act='recv_msg'
     AND %s),
     t2 AS
  (SELECT *,
          row_number() OVER (
                             ORDER BY sample_id) AS rank_id
   FROM t1),
 t3 AS
  (SELECT *
   FROM t2
   WHERE rank_id %% %d = %d ),
 x1 AS
  (SELECT split_part(snick, ':', 1) AS seller_nick,
          cnick,
          category,
          act,
          msg,
          remind_answer,
          cast(msg_time AS String) AS msg_time,
          question_b_qid,
          question_b_proba,
          MODE,
          DAY,
          create_time,
          is_robot_answer,
		  plat_goods_id,
		  current_sale_stage
   FROM dwd.mini_xdrs_log
   WHERE %s), 
 x2 AS
  (SELECT x1.*,
          t3.sample_id,
          if(x1.create_time = t3.create_time
             AND x1.act = 'recv_msg',1,0) AS flag
   FROM x1
  RIGHT JOIN [shuffle] t3 ON x1.seller_nick = t3.seller_nick
   AND x1.cnick = t3.cnick)
INSERT overwrite xd_tmp.algorithm_sample_data_all PARTITION (mission_id='%s')
SELECT x2.seller_nick,
       x2.cnick,
       x2.category,
       x2.act,
       x2.msg,
       x2.remind_answer,
       x2.msg_time,
       x2.question_b_qid,
       x2.question_b_proba,
       x2.MODE,
       x2.DAY,
       x2.create_time,
       x2.sample_id,
       x2.flag,
       xd_data.question_b.question,
       x2.is_robot_answer,
	   x2.plat_goods_id,
	   x2.current_sale_stage
FROM x2
LEFT JOIN [shuffle] xd_data.question_b ON cast(split_part(x2.question_b_qid, '.', 1) AS integer) = cast(split_part(xd_data.question_b.qid, '.', 1) AS integer);
`
	platform = "tb"
	query := u.createQuery(startDay, endDay, platform, groupList, shopList, qidList, filterRule, 0, 0)
	queryAll := u.createQuery(startDay, endDay, platform, groupList, shopList, nil, false, 0, 0)
	missionID := utils.ToMD5(fmt.Sprintf("%s %d", query, bucketSize))
	sql = fmt.Sprintf(sql, query, bucketSize, rand.Intn(int(bucketSize)), queryAll, missionID)
	log.Infof("CreateSamples sql: %v", sql)

	_, err, errCode := bdClient.PutBigData(
		context.Background(),
		u.Server,
		u.Module,
		sql,
		u.DBType)
	if err != nil {
		log.Errorf("[CreateSamples] GetBigData error %v err \n", err.Error())
		return "", err
	} else if errCode != 0 {
		log.Errorf("[CreateSamples] GetBigData errorCode %v\n", errCode)
		return "", fmt.Errorf("exception error code: %d", errCode)
	}

	//db := store.GetImpalaThing()
	//defer db.Close()
	//_, err := db.Execute(sql, missionID)
	//if err != nil {
	//	log.Errorf("execute sql fail: %v", err)
	//	return missionID, err
	//}

	return missionID, nil
}