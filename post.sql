insert into incidents (
        id, number, call_id_sys_user_id, description, short_description,
        configuration_item_service, configuration_item,
        impact, urgency, priority, channel, 
        assignment_group_id, assigned_sys_user_id, third_party_type, third_party_key, state,
        resolution_notes, category, sub_category, resolved_at, 
        opened_sys_user_id, opened_at, updated_by, updated_time, created_by, created_time )
select id, number, call_id_sys_user_id, description, short_description,
       configuration_item_service, configuration_item,
       impact, urgency, priority, channel,
       assignment_group_id, assigned_sys_user_id, third_party_type, third_party_key, state,
       resolution_notes, category, sub_category, resolved_at,
       opened_sys_user_id, opened_at, updated_by, updated_time, 'airflow', now()
from tmp_stg.sg_itsm
where filter = 'HPD' limit 10
*** end of HPD
insert into problem (
        id, number, short_description, description, state, 
        configuration_item_service, configuration_item, 
        impact, urgency, priority, category, sub_category, 
        third_party_type, third_party_key, workaround, fix_notes, 
        assignment_group_id, assigned_sys_user_id,
        opened_sys_user_id, opened_at, updated_by, updated_time, created_by, created_time )
select id, number, short_description, description, state, 
       configuration_item_service, configuration_item, 
       impact, urgency, priority, category, sub_category, 
       third_party_type, third_party_key, workaround, fix_notes, 
       assignment_group_id, assigned_sys_user_id,
       opened_sys_user_id, opened_at, updated_by, updated_time, 'airflow', now()
from tmp_stg.sg_itsm
where filter like 'PB_' limit 10
*** end of PB*
insert into change (
        id, number, requested_by_id, business_service_id,
        short_description, description,
        type, risk, state, category,
        start_date, end_date, work_start, work_end,
        assigned_to_user_id, opened_by_id, opened_at, created_by, created_time )
select id, number, requested_by_id, business_service_id,
       short_description, description,
       type, risk, state, category,
       start_date, end_date, work_start, work_end,
       assigned_to_user_id, opened_by_id, opened_at, 'airflow',now()
from tmp_stg.sg_itsm
where filter like 'CHG' limit 10
*** end of CHG
insert into request (
        id, number, item_for_user_id, item_open_by_user_id
        description, short_description, state,
        configuration_item, due_date,
        assignment_group, assigned_to, item_open_by_user_id, 
        open_date, updated_by, updated_time, created_by, created_time )
select id, number, item_for_user_id, item_open_by_user_id,
       description, short_description, state,
       configuration_item, due_date,
       assignment_group, assigned_to, open_by_user_id, 
       open_date, updated_by, updated_time, 'airflow', now()
from tmp_stg.sg_itsm
where filter like 'WOI' limit 10  
*** end of WOI / SQL