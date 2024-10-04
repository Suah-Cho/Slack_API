CHECK_DB = "SELECT 1;"

GET_ALL_USERS = "SELECT * FROM users;"

GET_DATAS_PER_ONE_DAY = """
with user_comp as (
	select u.name, c.comp_id
	from users u, compartments c 
	where u.user_id = c.owner_id
)

select u.name, g.comp_id, g.view_data_type_id, count(*) 
from gh_data_items g, user_comp u
where event_time >= now() - INTERVAL '1day' and u.comp_id = g.comp_id 
group by g.comp_id, g.view_data_type_id ;
"""

CONFIRM_DATA_AMOUNT = """
with all_combination as (
    select comp_id, x.selected_dates::date event_date
    from
    compartments,
    (SELECT generate_series('2024-09-21'::date, '2024-09-30'::date, '1 day'::interval) AS selected_dates) x
),get_data_type_mapping_count as (
    select
        b.comp_id,
        rgdi.event_time::date as event_date,
        count(*) as raw_gh_data_item_count
    from raw_gh_data_items rgdi,installed_devices b
    where
        rgdi.installed_device_id = b.installed_device_id
        and rgdi.event_time >= now() - interval '10 days'
    group by b.comp_id, event_date
), get_vender_data_type_mapping_count as (
    select
        b.comp_id,
        rcgdi.event_time::date as event_date,
        count(*) as raw_crawling_gh_data_items_count
    from raw_crawling_gh_data_items rcgdi, installed_devices b
    where
        rcgdi.installed_device_id = b.installed_device_id
        and rcgdi.event_time >= now() - interval '10 days'
    group by b.comp_id, event_date
), get_raw_to_gh_data_count as (
    select
        comp_id,
        gdi.event_time::date as event_date,
        count(*) as gh_data_item_count
    from gh_data_items gdi
    where
         gdi.event_time >= now() - interval '10 days'
    group by comp_id, event_date
), calc_count_and_ratio as (
SELECT
    xxx.comp_id,
    xxx.event_date,
    COALESCE(dtm.raw_gh_data_item_count, 0) AS raw_gh_data_item_count,
    COALESCE(vdtm.raw_crawling_gh_data_items_count, 0) AS raw_crawling_gh_data_items_count,
    COALESCE(rtg.gh_data_item_count, 0) AS gh_data_item_count
FROM
    all_combination xxx
    FULL OUTER JOIN get_data_type_mapping_count dtm
    on xxx.comp_id = dtm.comp_id and xxx.event_date = dtm.event_date
    FULL OUTER JOIN get_vender_data_type_mapping_count vdtm
    ON xxx.comp_id = vdtm.comp_id AND xxx.event_date = vdtm.event_date
    FULL OUTER JOIN get_raw_to_gh_data_count rtg
    ON xxx.comp_id = rtg.comp_id AND xxx.event_date = rtg.event_date
)
select
    c.name,
    CASE
        WHEN COALESCE(a.raw_crawling_gh_data_items_count, 0) > 0
        THEN (a.gh_data_item_count::float / a.raw_crawling_gh_data_items_count::float)::float
        ELSE 0
    END AS raw_crawl_to_gh_data_ratio,
    a.*
from calc_count_and_ratio a, compartments c
where a.comp_id = c.comp_id
order by  a.comp_id, event_date desc
"""


HEALTH_CHECK = """
with installed_device_last_health as (
      SELECT DISTINCT ON (idhc.installed_device_id) *
        FROM installed_device_health_checks idhc
        ORDER BY idhc.installed_device_id, idhc.create_ts desc
    )
    select
      idlh.id,
      idlh.installed_device_id,
      idlh.status,
      idlh.request_ts,
      idlh.create_ts,
      c.comp_id,
      c.name as comp_name,
      u.name as user_name,
      d.name as device_name
    from installed_device_last_health idlh, installed_devices id, compartments c, users u, devices d 
    where 
      idlh.installed_device_id = id.installed_device_id
      and id.comp_id = c.comp_id
      and id.device_id = d.device_id
      and c.owner_id = u.user_id
"""