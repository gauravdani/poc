with registered_user_events as (
select"$user_id" ,"$device_id", min("event_time") from joyn_snow.mixpanel_ch.screen_views 
where "$user_id" is not null
group by 1,2
),
anonymous_users_upgraded_to_registered_events as(
select a.*,coalesce(ru."$user_id",a."$user_id",a."$device_id") as "$modified_distinct_id" from joyn_snow.mixpanel_ch.screen_views a 
left join registered_user_events ru on a."$device_id"=ru."$device_id"
)
select last_day("event_time",'month') as CH_MONTH, count(distinct "$distinct_id") as users_without_identity_resolve,count(distinct "$modified_distinct_id") as users_with_identity_resolve from anonymous_users_upgraded_to_registered_events
group by 1 
union all
select last_day(base_date,'month'),null,count(distinct coalesce (user_id,session_installation_id)) as users_with_identity_resolve from joyn_snow.im_ch.screen_views_f
where base_date > '2024-03-31' and is_arbitrage_traffic = false
group by 1
order by 1 asc;

with registered_user_events as (
select"$user_id" ,"$device_id", min("event_time") from joyn_snow.mixpanel.screen_views 
where "$user_id" is not null
group by 1,2
),
anonymous_users_upgraded_to_registered_events as(
select a.*,coalesce(ru."$user_id",a."$user_id",a."$device_id") as "$modified_distinct_id" from joyn_snow.mixpanel.screen_views a 
left join registered_user_events ru on a."$device_id"=ru."$device_id"
)
select last_day("event_time",'month') as DE_MONTH, count(distinct "$distinct_id") as users_without_identity_resolve,count(distinct "$modified_distinct_id") as users_with_identity_resolve from anonymous_users_upgraded_to_registered_events
group by 1 
union all
select last_day(base_date,'month'),null,count(distinct coalesce (user_id,session_installation_id)) as users_with_identity_resolve from joyn_snow.im.screen_views_f
where base_date > '2024-03-31' and is_arbitrage_traffic = false
group by 1
order by 1 asc;


with registered_user_events as (
select"$user_id" ,"$device_id", min("event_time") from joyn_snow.mixpanel_at.screen_views 
where "$user_id" is not null
group by 1,2
),
anonymous_users_upgraded_to_registered_events as(
select a.*,coalesce(ru."$user_id",a."$user_id",a."$device_id") as "$modified_distinct_id" from joyn_snow.mixpanel_at.screen_views a 
left join registered_user_events ru on a."$device_id"=ru."$device_id"
)
select last_day("event_time",'month') as AT_MONTH, count(distinct "$distinct_id") as users_without_identity_resolve,count(distinct "$modified_distinct_id") as users_with_identity_resolve from anonymous_users_upgraded_to_registered_events
group by 1 
union all
select last_day(base_date,'month'),null,count(distinct coalesce (user_id,session_installation_id)) as users_with_identity_resolve from joyn_snow.im_at.screen_views_f
where base_date > '2024-03-31' and is_arbitrage_traffic = false
group by 1
order by 1 asc;
