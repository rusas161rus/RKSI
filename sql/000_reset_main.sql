-- Полная очистка объектов проекта (выполнять в БД apprksi)
BEGIN;

DROP VIEW IF EXISTS vw_schedule_resolved;
DROP VIEW IF EXISTS vw_all_schedule_for_ui;

DROP TABLE IF EXISTS parsed_schedule_entries CASCADE;
DROP TABLE IF EXISTS schedule_entries CASCADE;
DROP TABLE IF EXISTS site_admins CASCADE;
DROP TABLE IF EXISTS site_users CASCADE;
DROP TABLE IF EXISTS teachers CASCADE;
DROP TABLE IF EXISTS subjects CASCADE;
DROP TABLE IF EXISTS study_groups CASCADE;

COMMIT;
