/*
 * pre_deploy.sql
 * Create database, schema, and DCM project (runs before DCM deploy)
 */
CREATE DATABASE IF NOT EXISTS {{ db }}
    COMMENT = 'Synthetic Retail Bank - Development Database';

USE DATABASE {{ db }};

CREATE SCHEMA IF NOT EXISTS PUBLIC
    COMMENT = 'Default schema - DCM project container';

CREATE DCM PROJECT IF NOT EXISTS {{ db }}.PUBLIC.SYNTHETIC_RETAIL_BANK;
