with sessions_installation_id_user_id_count as (
select session_installation_id , count( distinct case when user_id is not null then user_id else null end) as counts from joyn_snow.im.screen_views_f
where base_date > '2023-12-31' and is_arbitrage_traffic=false
group by 1)
select count(*) from sessions_installation_id_user_id_count where counts = 1;


with sessions_installation_id_user_id_count as (
select session_installation_id , count( distinct case when user_id is not null then user_id else null end) as counts from joyn_snow.im.screen_views_f
where base_date > '2022-12-31' and is_arbitrage_traffic=false
group by 1)
select case when counts>1 then 'greater than 1' when counts=1 then 'equal to 1' else 'weird' end as category,  count(*) from sessions_installation_id_user_id_count group by 1;