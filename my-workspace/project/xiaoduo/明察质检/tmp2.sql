(
    ('{{ alert_state=-1 }}' = '-1')
    OR
    ('{{ alert_state=-1 }}' = '0' AND is_finished!='True' AND toString(status)!='2')
    OR
    ('{{ alert_state=-1 }}' = '1' AND is_finished='True')
    OR
    ('{{ alert_state=-1 }}' = '2' AND is_finished!='True' AND toString(status)='2')
)