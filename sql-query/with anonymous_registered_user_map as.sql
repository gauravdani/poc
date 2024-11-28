WITH anonymous_registered_user_map AS (
    SELECT
        "$user_id" AS user_id,
        "$device_id" AS anonymous_id
    FROM
        joyn_snow.mixpanel.screen_views
) , 
update_screen_views AS (
    SELECT
        b.user_id AS user_id,
        A."$device_id" as anonymous_id 
    FROM
        joyn_snow.mixpanel.screen_views A
        LEFT JOIN anonymous_registered_user_map b
        ON A."$device_id" = b.anonymous_id
)
SELECT count(distrinct coalesce(user_id,anonymous_id)) from update_screen_views
