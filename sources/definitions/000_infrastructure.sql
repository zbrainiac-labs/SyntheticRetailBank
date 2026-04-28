DEFINE SCHEMA {{ db }}.{{ cmd_raw }}
    COMMENT = 'Commodity raw data - energy, metals, agricultural trades';

DEFINE SCHEMA {{ db }}.{{ crm_raw }}
    COMMENT = 'CRM raw data - customer/party information and accounts';

DEFINE SCHEMA {{ db }}.{{ eqt_raw }}
    COMMENT = 'Equity trading raw data - FIX protocol trades';

DEFINE SCHEMA {{ db }}.{{ fii_raw }}
    COMMENT = 'Fixed income raw data - bonds and interest rate swaps';

DEFINE SCHEMA {{ db }}.{{ loa_raw }}
    COMMENT = 'Loan raw data - loan information and mortgage documents';

DEFINE SCHEMA {{ db }}.{{ pay_raw }}
    COMMENT = 'Payment raw data - transactions and SWIFT ISO20022 messages';

DEFINE SCHEMA {{ db }}.{{ ref_raw }}
    COMMENT = 'Reference data - FX rates and lookup tables';

DEFINE SCHEMA {{ db }}.{{ rep_raw }}
    COMMENT = 'Reporting raw data - FINMA LCR, BCBS239, HQLA holdings';

DEFINE SCHEMA {{ db }}.{{ cmd_agg }}
    COMMENT = 'Commodity aggregation - delta risk and volatility analytics';

DEFINE SCHEMA {{ db }}.{{ crm_agg }}
    COMMENT = 'CRM aggregation - customer 360 views, SCD Type 2';

DEFINE SCHEMA {{ db }}.{{ eqt_agg }}
    COMMENT = 'Equity trading aggregation - trade analytics and portfolio positions';

DEFINE SCHEMA {{ db }}.{{ fii_agg }}
    COMMENT = 'Fixed income aggregation - duration, DV01, credit risk analytics';

DEFINE SCHEMA {{ db }}.{{ loa_agg }}
    COMMENT = 'Loan aggregation - loan analytics and reporting';

DEFINE SCHEMA {{ db }}.{{ pay_agg }}
    COMMENT = 'Payment aggregation - anomaly detection, SWIFT message processing';

DEFINE SCHEMA {{ db }}.{{ ref_agg }}
    COMMENT = 'Reference data aggregation - enhanced FX rates and analytics';

DEFINE SCHEMA {{ db }}.{{ rep_agg }}
    COMMENT = 'Reporting aggregation - FINMA LCR, BCBS239, FRTB';

DEFINE TAG {{ db }}.PUBLIC.SENSITIVITY_LEVEL
    COMMENT = 'Data sensitivity classification: restricted | top_secret';
