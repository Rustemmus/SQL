INSERT INTO AGG_SERVICE_SUBSCRIPTION
        SELECT to_date('01.10.2016','dd.mm.yyyy') date_key
             , market_key
             , -99 channel_key
             , curr_price_plan_key
             , -99 msisdn_type
             , network_type
             , account_type_key
             , NULL subs_lifespan
             , soc_code
             , service_type
             , connected_operator_key
             , -99 business_service_key
             , sum(beg_cnt)
             , sum(inc_cnt)
             , sum(dec_cnt)
             , sum(end_cnt)
             , sysdate load_date
          FROM (  -- begin
                  SELECT T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                       , count( * ) beg_cnt
                       , 0 inc_cnt
                       , 0 dec_cnt
                       , 0 end_cnt
                    FROM DWH.FCT_SERV_SUBSCRIPTION T
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE T.ban_key = s.ban_key
                     AND T.subs_key = s.subs_key
                     AND T.soc_effective_date < to_date('01.10.2016','dd.mm.yyyy')
                     AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                     AND T.soc_code <> 'AO'
                GROUP BY T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                UNION ALL
                  -- end
                  SELECT T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                       , 0 beg_cnt
                       , 0 inc_cnt
                       , 0 dec_cnt
                       , count( * ) end_cnt
                    FROM DWH.FCT_SERV_SUBSCRIPTION T
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE T.ban_key = s.ban_key
                     AND T.subs_key = s.subs_key
                     AND T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                     AND T.soc_expiration_date > add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                     AND T.soc_code <> 'AO'
                GROUP BY T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                UNION ALL
                  -- in
                  SELECT T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                       , 0 beg_cnt
                       , count( * ) inc_cnt
                       , 0 dec_cnt
                       , 0 end_cnt
                    FROM DWH.FCT_SERV_SUBSCRIPTION T
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE T.ban_key = s.ban_key
                     AND T.subs_key = s.subs_key
                     AND T.soc_effective_date >= to_date('01.10.2016','dd.mm.yyyy')
                     AND T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                     AND T.soc_effective_date < T.soc_expiration_date
                     AND T.soc_code <> 'AO'
                GROUP BY T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                UNION ALL
                  -- out
                  SELECT T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                       , 0 beg_cnt
                       , 0 inc_cnt
                       , count( * ) dec_cnt
                       , 0 end_cnt
                    FROM DWH.FCT_SERV_SUBSCRIPTION T
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE T.ban_key = s.ban_key
                     AND T.subs_key = s.subs_key
                     AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                     AND T.soc_expiration_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                     AND T.soc_effective_date < T.soc_expiration_date
                     AND T.soc_code <> 'AO'
                GROUP BY T.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , T.soc_code
                       , T.service_type
                       , T.connected_operator_key
                UNION ALL
                  -- ADSL
                  -- begin
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'ADSLGroup' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , count( * ) beg_cnt
                       , 0 inc_cnt
                       , 0 dec_cnt
                       , 0 end_cnt
                    FROM (SELECT DISTINCT T.ban_key
                                        , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date < to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'ADSL%') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                UNION ALL
                  -- end
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'ADSLGroup' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , 0 beg_cnt
                       , 0 inc_cnt
                       , 0 dec_cnt
                       , count( * ) end_cnt
                    FROM (SELECT DISTINCT T.ban_key
                                        , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_expiration_date > add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'ADSL%') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                UNION ALL
                  -- in
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'ADSLGroup' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , 0 beg_cnt
                       , count( * ) inc_cnt
                       , 0 dec_cnt
                       , 0 end_cnt
                    FROM (SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_effective_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_effective_date < T.soc_expiration_date
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'ADSL%'
                          MINUS
                          SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date < to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'ADSL%') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                UNION ALL
                  -- out
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'ADSLGroup' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , 0 beg_cnt
                       , 0 inc_cnt
                       , count( * ) dec_cnt
                       , 0 end_cnt
                    FROM (SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_expiration_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_effective_date < T.soc_expiration_date
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'ADSL%'
                          MINUS
                          SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_expiration_date > add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'ADSL%') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                UNION ALL
                  -- AO
                  -- begin
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'AO' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , count( * ) beg_cnt
                       , 0 inc_cnt
                       , 0 dec_cnt
                       , 0 end_cnt
                    FROM (SELECT DISTINCT T.ban_key
                                        , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date < to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'AO') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                UNION ALL
                  -- end
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'AO' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , 0 beg_cnt
                       , 0 inc_cnt
                       , 0 dec_cnt
                       , count( * ) end_cnt
                    FROM (SELECT DISTINCT T.ban_key
                                        , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_expiration_date > add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'AO') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                UNION ALL
                  -- in
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'AO' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , 0 beg_cnt
                       , count( * ) inc_cnt
                       , 0 dec_cnt
                       , 0 end_cnt
                    FROM (SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_effective_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_effective_date < T.soc_expiration_date
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'AO'
                          MINUS
                          SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date < to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'AO') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                UNION ALL
                  -- out
                  SELECT s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key
                       , 'AO' soc_code
                       , 'O' service_type
                       , -99 connected_operator_key
                       , 0 beg_cnt
                       , 0 inc_cnt
                       , count( * ) dec_cnt
                       , 0 end_cnt
                    FROM (SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_expiration_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_expiration_date >= to_date('01.10.2016','dd.mm.yyyy')
                             AND T.soc_effective_date < T.soc_expiration_date
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'AO'
                          MINUS
                          SELECT T.ban_key
                               , T.subs_key
                            FROM DWH.FCT_SERV_SUBSCRIPTION T
                           WHERE T.soc_effective_date <= add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.soc_expiration_date > add_months(to_date('01.10.2016','dd.mm.yyyy'), 1) - 1
                             AND T.service_type = 'O'
                             AND T.soc_code LIKE 'AO') t1
                       , DWH.DIM_SUBSCRIBERS s
                   WHERE t1.ban_key = s.ban_key
                     AND t1.subs_key = s.subs_key
                GROUP BY s.market_key
                       , s.curr_price_plan_key
                       , s.network_type
                       , s.account_type_key)
      GROUP BY market_key
             , curr_price_plan_key
             , network_type
             , account_type_key
             , soc_code
             , service_type
             , connected_operator_key;

   COMMIT;
