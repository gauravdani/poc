CREATE TABLE HIGHTOUCH_TEST.user_profiles_f AS 
WITH highlight_shows AS (
  SELECT 
    COALESCE(media_id, gracenote_id) AS media_id, 
    tvshow_title 
  FROM 
    "JOYN_SNOW".IM_MAIN.ASSET_D 
  WHERE 
    LOWER(tvshow_title) LIKE ANY(
      '%germany''s next topmodel%', '%schlag den star%', 
      '%wer stiehlt mir die show%', '%wer isses%', 
      '%the masked singer%', '%joko & klaas gegen prosieben%', 
      '%autoball%', '%stars in der manege%', 
      '%the voice kids%', '%the voice of germany%', 
      '%das 1% quiz%', '%kiwis große partynacht%', 
      '%the floor%', '%das große allgemeinwissens%', 
      '%hast du töne%'
    )
), 
consent_data AS (
  SELECT 
    trim(RECORD_CONTENT : user_id :: STRING) AS user_id, 
    RECORD_CONTENT : amazon_audiences :: BOOLEAN AS amazon_audiences, 
    RECORD_CONTENT : braze :: BOOLEAN AS braze, 
    RECORD_CONTENT : google_ads_audiences :: BOOLEAN AS google_ads_audiences, 
    RECORD_CONTENT : google_enhanced_audiences :: BOOLEAN AS google_enhanced_audiences, 
    RECORD_CONTENT : hightouch :: BOOLEAN AS hightouch, 
    RECORD_CONTENT : meta_custom_audiences :: BOOLEAN AS meta_custom_audiences,
    RECORD_CONTENT : ingestion_time :: DATE as ingestion_time,
    RECORD_CONTENT : device_platform :: string as device_platform,
    RECORD_CONTENT : device_platform :: string as device_type
  FROM 
    joyn_snow.kafka.prd_consent_updated_v1 
  WHERE 
    RECORD_CONTENT : ingestion_time :: DATE > '2024-06-30'
    and (record_content:url::string IS NULL OR record_content:url::string not like '%7pass%')
  QUALIFY ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY
                    ingestion_time DESC
            ) = 1
), 
vv_epg_extended AS (
  SELECT 
    user_id, 
    entitlement_id, 
    total_duration_sec, 
    content_owner, 
    media_id, 
    livestream_type, 
    started_ads_preroll, 
    started_ads_midroll, 
    asset_id, 
    base_date, 
    gracenote_id, 
    distribution_tenant 
  FROM 
    joyn_snow.im_main.video_views_epg_extended 
  WHERE 
    base_date > '2024-06-30' 
), 
email_hashes AS (
  SELECT 
    DISTINCT record_content : accountData : sevenpassEmailHash :: string AS email_hash, 
    TRIM(record_content : joynId :: string) AS user_id 
  FROM 
    joyn_snow.edw.user_s 
  WHERE 
    email_hash IS NOT NULL
), 
genre_list AS (
  SELECT 
    DISTINCT asset_id, 
    LISTAGG(DISTINCT genre_name, ', ') WITHIN GROUP (
      ORDER BY 
        genre_name ASC
    ) AS genre_list 
  FROM 
    joyn_snow.im_main.asset_genre_d 
  GROUP BY 
    asset_id
), 
new_users_last_14_days AS (
  SELECT 
    "$distinct_id" AS user_id, 
    TRUE AS new_user_last_14D 
  FROM 
    joyn_snow.mixpanel.account_created_non_pii 
  WHERE 
    "time" >= CURRENT_DATE - INTERVAL '14 DAYS' 
  GROUP BY 
    user_id
), 
vv_last_14_days AS (
  SELECT 
    a.user_id, 
    COUNT(DISTINCT a.entitlement_id) AS vv_last_14D, 
    SUM(a.total_duration_sec) AS wt_last_14D, 
    COUNT(
      CASE WHEN a.CONTENT_OWNER = 'P7S1' THEN 1 END
    ) AS VV_P7S1_CONTENT_14D, 
    COUNT(
      CASE WHEN a.CONTENT_OWNER = 'JOYN' THEN 1 END
    ) AS VV_JOYN_CONTENT_14D, 
    COUNT(
      CASE WHEN b.genre_list LIKE '%reality%' THEN 1 END
    ) AS VV_REALITY_14D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%comedy%' THEN 1 END
    ) AS VV_COMEDY_14D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%fiction%' THEN 1 END
    ) AS VV_FICTION_14D, 
    COUNT(
      CASE WHEN a.media_id IS NOT NULL THEN 1 END
    ) AS VV_VOD_14D, 
    COUNT(
      CASE WHEN livestream_type != 'odc' THEN 1 END
    ) AS VV_LIVE_14D, 
    sum(A.STARTED_ADS_PREROLL) + sum(A.STARTED_ADS_MIDROLL) AS ADS_14D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%anime%' THEN 1 END
    ) AS VV_ANIME_14D, 
    COUNT(
      CASE WHEN c.media_id IS NOT NULL 
      or d.media_id is not null THEN 1 END
    ) AS VV_SHOW_14D 
  FROM 
    vv_epg_extended a 
    LEFT JOIN genre_list B ON A.ASSET_ID = B.ASSET_ID 
    LEFT JOIN highlight_shows c ON a.media_id = c.media_id 
    LEFT JOIN highlight_shows d ON a.gracenote_id = d.media_id 
  WHERE 
    a.base_date >= CURRENT_DATE - INTERVAL '14 DAYS' 
  GROUP BY 
    a.user_id
), 
active_days_last_14_days AS (
  SELECT 
    user_id, 
    COUNT(DISTINCT base_date) AS active_days_last_14D, 
  FROM 
    joyn_snow.im_main.screen_views_f 
  WHERE 
    base_date >= CURRENT_DATE - INTERVAL '14 DAYS' 
  GROUP BY 
    user_id
), 
vv_last_30_days AS (
  SELECT 
    a.user_id, 
    COUNT(DISTINCT a.entitlement_id) AS vv_last_30D, 
    sum(a.total_duration_sec) AS wt_last_30D, 
    COUNT(
      CASE WHEN CONTENT_OWNER = 'P7S1' THEN 1 END
    ) AS VV_P7S1_CONTENT_30D, 
    COUNT(
      CASE WHEN a.CONTENT_OWNER = 'JOYN' THEN 1 END
    ) AS VV_JOYN_CONTENT_30D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%reality%' THEN 1 END
    ) AS VV_REALITY_30D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%comedy%' THEN 1 END
    ) AS VV_COMEDY_30D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%fiction%' THEN 1 END
    ) AS VV_FICTION_30D, 
    COUNT(
      CASE WHEN a.media_id IS NOT NULL THEN 1 END
    ) AS VV_VOD_30D, 
    COUNT(
      CASE WHEN livestream_type != 'odc' THEN 1 END
    ) AS VV_LIVE_30D, 
    sum(A.STARTED_ADS_PREROLL) + sum(A.STARTED_ADS_MIDROLL) AS ADS_30D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%anime%' THEN 1 END
    ) AS VV_ANIME_30D, 
    COUNT(
      CASE WHEN c.media_id IS NOT NULL 
      or d.media_id IS NOT NULL THEN 1 END
    ) AS VV_SHOW_30D 
  FROM 
    vv_epg_extended A 
    LEFT JOIN genre_list B ON A.ASSET_ID = B.ASSET_ID 
    LEFT JOIN highlight_shows c ON A.media_id = c.media_id 
    LEFT JOIN highlight_shows d ON A.gracenote_id = d.media_id 
  WHERE 
    A.base_date >= CURRENT_DATE - INTERVAL '30 DAYS' 
  GROUP BY 
    a.user_id
), 
vv_last_45_days AS (
  SELECT 
    a.user_id, 
    COUNT(DISTINCT a.entitlement_id) AS vv_last_45D, 
    sum(a.total_duration_sec) AS wt_last_45D, 
    COUNT(
      CASE WHEN a.CONTENT_OWNER = 'P7S1' THEN 1 END
    ) AS VV_P7S1_CONTENT_45D, 
    COUNT(
      CASE WHEN a.CONTENT_OWNER = 'JOYN' THEN 1 END
    ) AS VV_JOYN_CONTENT_45D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%reality%' THEN 1 END
    ) AS VV_REALITY_45D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%comedy%' THEN 1 END
    ) AS VV_COMEDY_45D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%fiction%' THEN 1 END
    ) AS VV_FICTION_45D, 
    COUNT(
      CASE WHEN a.media_id IS NOT NULL THEN 1 END
    ) AS VV_VOD_45D, 
    COUNT(
      CASE WHEN livestream_type != 'odc' THEN 1 END
    ) AS VV_LIVE_45D, 
    sum(A.STARTED_ADS_PREROLL) + sum(A.STARTED_ADS_MIDROLL) AS ADS_45D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%anime%' THEN 1 END
    ) AS VV_ANIME_45D, 
    COUNT(
      CASE WHEN c.media_id IS NOT NULL 
      or d.media_id IS NOT NULL THEN 1 END
    ) AS VV_SHOW_45D 
  FROM 
    vv_epg_extended A 
    LEFT JOIN genre_list B ON A.ASSET_ID = B.ASSET_ID 
    LEFT JOIN highlight_shows c ON A.media_id = c.media_id 
    LEFT JOIN highlight_shows d ON A.gracenote_id = d.media_id 
  WHERE 
    A.base_date >= CURRENT_DATE - INTERVAL '45 DAYS' 
  GROUP BY 
    a.user_id
), 
vv_last_60_days AS (
  SELECT 
    a.user_id, 
    COUNT(DISTINCT a.entitlement_id) AS vv_last_60D, 
    sum(a.total_duration_sec) AS wt_last_60D, 
    COUNT(
      CASE WHEN a.CONTENT_OWNER = 'P7S1' THEN 1 END
    ) AS VV_P7S1_CONTENT_60D, 
    COUNT(
      CASE WHEN a.CONTENT_OWNER = 'JOYN' THEN 1 END
    ) AS VV_JOYN_CONTENT_60D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%reality%' THEN 1 END
    ) AS VV_REALITY_60D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%comedy%' THEN 1 END
    ) AS VV_COMEDY_60D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%fiction%' THEN 1 END
    ) AS VV_FICTION_60D, 
    COUNT(
      CASE WHEN a.media_id IS NOT NULL THEN 1 END
    ) AS VV_VOD_60D, 
    COUNT(
      CASE WHEN livestream_type != 'odc' THEN 1 END
    ) AS VV_LIVE_60D, 
    sum(A.STARTED_ADS_PREROLL) + sum(A.STARTED_ADS_MIDROLL) AS ADS_60D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%anime%' THEN 1 END
    ) AS VV_ANIME_60D, 
    COUNT(
      CASE WHEN c.media_id IS NOT NULL 
      or d.media_id IS NOT NULL THEN 1 END
    ) AS VV_SHOW_60D 
  FROM 
    vv_epg_extended A 
    LEFT JOIN genre_list B ON A.ASSET_ID = B.ASSET_ID 
    LEFT JOIN highlight_shows c ON A.media_id = c.media_id 
    LEFT JOIN highlight_shows d ON A.gracenote_id = d.media_id 
  WHERE 
    base_date >= CURRENT_DATE - INTERVAL '60 DAYS' 
  GROUP BY 
    user_id
), 
vv_last_90_days AS (
  SELECT 
    A.user_id, 
    COUNT(DISTINCT A.entitlement_id) AS vv_last_90D, 
    sum(A.total_duration_sec) AS wt_last_90D, 
    COUNT(
      CASE WHEN A.CONTENT_OWNER = 'P7S1' THEN 1 END
    ) AS VV_P7S1_CONTENT_90D, 
    COUNT(
      CASE WHEN A.CONTENT_OWNER = 'JOYN' THEN 1 END
    ) AS VV_JOYN_CONTENT_90D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%reality%' THEN 1 END
    ) AS VV_REALITY_90D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%comedy%' THEN 1 END
    ) AS VV_COMEDY_90D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%fiction%' THEN 1 END
    ) AS VV_FICTION_90D, 
    COUNT(
      CASE WHEN a.media_id IS NOT NULL THEN 1 END
    ) AS VV_VOD_90D, 
    COUNT(
      CASE WHEN livestream_type != 'odc' THEN 1 END
    ) AS VV_LIVE_90D, 
    sum(A.STARTED_ADS_PREROLL) + sum(A.STARTED_ADS_MIDROLL) AS ADS_90D, 
    COUNT(
      CASE WHEN lower(b.genre_list) LIKE '%anime%' THEN 1 END
    ) AS VV_ANIME_90D, 
    COUNT(
      CASE WHEN c.media_id IS NOT NULL 
      or d.media_id IS NOT NULL THEN 1 END
    ) AS VV_SHOW_90D 
  FROM 
    vv_epg_extended A 
    LEFT JOIN genre_list B ON A.ASSET_ID = B.ASSET_ID 
    LEFT JOIN highlight_shows c ON A.media_id = c.media_id 
    LEFT JOIN highlight_shows d ON A.gracenote_id = d.media_id 
  WHERE 
    A.base_date >= CURRENT_DATE - INTERVAL '90 DAYS' 
  GROUP BY 
    A.user_id
), 
user_profiles AS (
  SELECT 
    DISTINCT a.user_id, 
    z.email_hash, 
    a.total_video_views, 
    a.total_watchtime_sec, 
    b.active_days_last_14D, 
    e.new_user_last_14D, 
    c.vv_last_14D, 
    c.wt_last_14D, 
    c.VV_P7S1_CONTENT_14D, 
    c.VV_JOYN_CONTENT_14D, 
    c.VV_REALITY_14D, 
    c.VV_COMEDY_14D, 
    c.VV_VOD_14D, 
    c.VV_LIVE_14D, 
    c.VV_FICTION_14D, 
    c.ADS_14D, 
    c.VV_SHOW_14D, 
    f.vv_last_30D, 
    f.wt_last_30D, 
    f.VV_P7S1_CONTENT_30D, 
    f.VV_JOYN_CONTENT_30D, 
    f.VV_REALITY_30D, 
    f.VV_COMEDY_30D, 
    f.VV_VOD_30D, 
    f.VV_LIVE_30D, 
    f.VV_FICTION_30D, 
    f.ADS_30D, 
    f.VV_ANIME_30D, 
    f.VV_SHOW_30D, 
    g.vv_last_45D, 
    g.wt_last_45D, 
    g.VV_P7S1_CONTENT_45D, 
    g.VV_JOYN_CONTENT_45D, 
    g.VV_REALITY_45D, 
    g.VV_COMEDY_45D, 
    g.VV_VOD_45D, 
    g.VV_LIVE_45D, 
    g.VV_FICTION_45D, 
    g.VV_ANIME_45D, 
    g.VV_SHOW_45D, 
    g.ADS_45D, 
    h.vv_last_60D, 
    h.wt_last_60D, 
    h.VV_P7S1_CONTENT_60D, 
    h.VV_JOYN_CONTENT_60D, 
    h.VV_REALITY_60D, 
    h.VV_COMEDY_60D, 
    h.VV_VOD_60D, 
    h.VV_LIVE_60D, 
    h.VV_FICTION_60D, 
    h.ADS_60D, 
    h.VV_ANIME_60D, 
    h.VV_SHOW_60D, 
    i.vv_last_90D, 
    i.wt_last_90D, 
    i.VV_P7S1_CONTENT_90D, 
    i.VV_JOYN_CONTENT_90D, 
    i.VV_REALITY_90D, 
    i.VV_COMEDY_90D, 
    i.VV_VOD_90D, 
    i.VV_LIVE_90D, 
    i.VV_FICTION_90D, 
    i.VV_ANIME_90D, 
    i.VV_SHOW_90D, 
    i.ADS_90D, 
    cd.amazon_audiences, 
    cd.google_ads_audiences, 
    cd.google_enhanced_audiences, 
    cd.meta_custom_audiences, 
    cd.braze 
  FROM 
    joyn_snow.im_main.user_factsheet_f a 
    INNER JOIN email_hashes z ON a.user_id = z.user_id AND take_seconds_rule = 'all' 
    INNER JOIN consent_data cd ON cd.user_id = a.user_id
    LEFT JOIN active_days_last_14_days b ON a.user_id = b.user_id 
    LEFT JOIN new_users_last_14_days e ON a.user_id = e.user_id 
    LEFT JOIN vv_last_14_days c ON a.user_id = c.user_id 
    LEFT JOIN vv_last_30_days f ON a.user_id = f.user_id 
    LEFT JOIN vv_last_45_days g ON a.user_id = g.user_id 
    LEFT JOIN vv_last_60_days h ON a.user_id = h.user_id 
    LEFT JOIN vv_last_90_days i ON a.user_id = i.user_id
) 
SELECT 
  * 
FROM 
  user_profiles
where user_id like 'JNDE%' and length(email_hash)>0;