DROP TABLE IF EXISTS main.ip_visits;

DROP TABLE IF EXISTS main.er_visits;

DROP TABLE IF EXISTS main.op_visits;

DROP TABLE IF EXISTS main.all_visits;

/* inpatient visits */
/* collapse ip claim lines with <=1 day between them into one visit */
CREATE TABLE main.ip_visits AS WITH cte_end_dates AS (
    select
        patient,
        encounterclass,
        (event_date + -1 * INTERVAL '1 day') as end_date
    from
        (
            select
                patient,
                encounterclass,
                event_date,
                event_type,
                max(start_ordinal) over (
                    partition by patient,
                    encounterclass
                    order by
                        event_date,
                        event_type rows unbounded preceding
                ) as start_ordinal,
                row_number() over (
                    partition by patient,
                    encounterclass
                    order by
                        event_date,
                        event_type
                ) as overall_ord
            from
                (
                    select
                        patient,
                        encounterclass,
                        start as event_date,
                        -1 as event_type,
                        row_number () over (
                            partition by patient,
                            encounterclass
                            order by
                                start,
                                stop
                        ) as start_ordinal
                    from
                        synthea.encounters
                    where
                        encounterclass = 'inpatient'
                    union
                    all
                    select
                        patient,
                        encounterclass,
                        (stop + 1 * INTERVAL '1 day'),
                        1 as event_type,
                        null
                    from
                        synthea.encounters
                    where
                        encounterclass = 'inpatient'
                ) rawdata
        ) e
    where
        (2 * e.start_ordinal - e.overall_ord = 0)
),
cte_visit_ends as (
    select
        min(v.id) encounter_id,
        v.patient,
        v.encounterclass,
        v.start visit_start_date,
        min(e.end_date) as visit_end_date
    from
        synthea.encounters v
        join cte_end_dates e on v.patient = e.patient
        and v.encounterclass = e.encounterclass
        and e.end_date >= v.start
    group by
        v.patient,
        v.encounterclass,
        v.start
)
SELECT
    t2.encounter_id,
    t2.patient,
    t2.encounterclass,
    t2.visit_start_date,
    t2.visit_end_date
FROM
    (
        select
            encounter_id,
            patient,
            encounterclass,
            min(visit_start_date) as visit_start_date,
            visit_end_date
        from
            cte_visit_ends
        group by
            encounter_id,
            patient,
            encounterclass,
            visit_end_date
    ) t2;

/* emergency visits */
/* collapse er claim lines with no days between them into one visit */
CREATE TABLE main.er_visits AS
SELECT
    t2.encounter_id,
    t2.patient,
    t2.encounterclass,
    t2.visit_start_date,
    t2.visit_end_date
FROM
    (
        select
            min(encounter_id) encounter_id,
            patient,
            encounterclass,
            visit_start_date,
            max(visit_end_date) as visit_end_date
        from
            (
                select
                    cl1.id encounter_id,
                    cl1.patient,
                    cl1.encounterclass,
                    cl1.start visit_start_date,
                    cl2.stop visit_end_date
                from
                    synthea.encounters cl1
                    join synthea.encounters cl2 on cl1.patient = cl2.patient
                    and cl1.start = cl2.start
                    and cl1.encounterclass = cl2.encounterclass
                where
                    cl1.encounterclass in ('emergency', 'urgent')
            ) t1
        group by
            patient,
            encounterclass,
            visit_start_date
    ) t2;

/* outpatient visits */
CREATE TABLE main.op_visits AS WITH cte_visits_distinct AS (
    select
        min(id) encounter_id,
        patient,
        encounterclass,
        start visit_start_date,
        stop visit_end_date
    from
        synthea.encounters
    where
        encounterclass in ('ambulatory', 'wellness', 'outpatient')
    group by
        patient,
        encounterclass,
        start,
        stop
)
SELECT
    min(encounter_id) encounter_id,
    patient,
    encounterclass,
    visit_start_date,
    max(visit_end_date) as visit_end_date
FROM
    cte_visits_distinct
group by
    patient,
    encounterclass,
    visit_start_date;

/* all visits */
CREATE TABLE main.all_visits AS
SELECT
    *,
    row_number() over(
        order by
            patient
    ) as visit_occurrence_id
FROM
    (
        select
            *
        from
            main.ip_visits
        union
        all
        select
            *
        from
            main.er_visits
        union
        all
        select
            *
        from
            main.op_visits
    ) t1;

DROP TABLE IF EXISTS main.ip_visits;

DROP TABLE IF EXISTS main.er_visits;

DROP TABLE IF EXISTS main.op_visits;

/*assign visit_occurrence_id to all encounters*/
DROP TABLE IF EXISTS main.assign_all_visit_ids;

CREATE TABLE main.assign_all_visit_ids AS
SELECT
    e.id as encounter_id,
    e.patient as person_source_value,
    e.start as date_service,
    e.stop as date_service_end,
    e.encounterclass,
    av.encounterclass as visit_type,
    av.visit_start_date,
    av.visit_end_date,
    av.visit_occurrence_id,
    case
        when e.encounterclass = 'inpatient'
        and av.encounterclass = 'inpatient' then visit_occurrence_id
        when e.encounterclass in ('emergency', 'urgent') then (
            case
                when av.encounterclass = 'inpatient'
                and e.start > av.visit_start_date then visit_occurrence_id
                when av.encounterclass in ('emergency', 'urgent')
                and e.start = av.visit_start_date then visit_occurrence_id
                else null
            end
        )
        when e.encounterclass in ('ambulatory', 'wellness', 'outpatient') then (
            case
                when av.encounterclass = 'inpatient'
                and e.start >= av.visit_start_date then visit_occurrence_id
                when av.encounterclass in ('ambulatory', 'wellness', 'outpatient') then visit_occurrence_id
                else null
            end
        )
        else null
    end as visit_occurrence_id_new
FROM
    synthea.encounters e
    join main.all_visits av on e.patient = av.patient
    and e.start >= av.visit_start_date
    and e.start <= av.visit_end_date;

DROP TABLE IF EXISTS main.final_visit_ids;

create table main.final_visit_ids as
select
    encounter_id,
    visit_occurrence_id_new
from
(
        select
            *,
            row_number () over (
                partition by encounter_id
                order by
                    priority
            ) as rn
        from
            (
                select
                    *,
                    case
                        when encounterclass in ('emergency', 'urgent') then (
                            case
                                when visit_type = 'inpatient'
                                and visit_occurrence_id_new is not null then 1
                                when visit_type in ('emergency', 'urgent')
                                and visit_occurrence_id_new is not null then 2
                                else 99
                            end
                        )
                        when encounterclass in ('ambulatory', 'wellness', 'outpatient') then (
                            case
                                when visit_type = 'inpatient'
                                and visit_occurrence_id_new is not null then 1
                                when visit_type in ('ambulatory', 'wellness', 'outpatient')
                                and visit_occurrence_id_new is not null then 2
                                else 99
                            end
                        )
                        when encounterclass = 'inpatient'
                        and visit_type = 'inpatient'
                        and visit_occurrence_id_new is not null then 1
                        else 99
                    end as priority
                from
                    main.assign_all_visit_ids
            ) t1
    ) t2
where
    rn = 1
