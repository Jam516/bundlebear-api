from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
import snowflake.connector
from snowflake.connector import DictCursor
import os

REDIS_LINK = os.environ['REDIS']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASS = os.environ['SNOWFLAKE_PASS']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']

config = {
  "CACHE_TYPE": "redis",
  "CACHE_DEFAULT_TIMEOUT": 3600,
  "CACHE_REDIS_URL": REDIS_LINK
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
CORS(app)


def make_cache_key(*args, **kwargs):
  path = request.path
  args = str(hash(frozenset(request.args.items())))
  return (path + args).encode('utf-8')


def execute_sql(sql_string, **kwargs):
  conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="BUNDLEBEAR",
                                     schema="DBT_KOFI")

  sql = sql_string.format(**kwargs)
  # res = conn.cursor(DictCursor).execute(sql)
  # results = res.fetchall()
  # conn.close()
  try:
    res = conn.cursor(DictCursor).execute(sql)
    results = res.fetchall()
  except Exception as e:
    print(f"An error occurred while executing the SQL query: {sql}")
    raise e
  finally:
    conn.close()
  return results


@app.route('/overview')
@cache.memoize(make_name=make_cache_key)
def index():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if chain == 'all':
    stat_deployments = execute_sql('''
    SELECT 
    SUM(NUM_DEPLOYED) as NUM_DEPLOYMENTS
    FROM 
    (
      SELECT 
      COUNT(*) AS NUM_DEPLOYED
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ACCOUNT_DEPLOYMENTS
      UNION ALL SELECT 
      COUNT(*) AS NUM_DEPLOYED
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS
      UNION ALL SELECT 
      COUNT(*) AS NUM_DEPLOYED
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ACCOUNT_DEPLOYMENTS
      UNION ALL SELECT 
      COUNT(*) AS NUM_DEPLOYED
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ACCOUNT_DEPLOYMENTS
      UNION ALL SELECT 
      COUNT(*) AS NUM_DEPLOYED
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ACCOUNT_DEPLOYMENTS
      UNION ALL SELECT 
      COUNT(*) AS NUM_DEPLOYED
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ACCOUNT_DEPLOYMENTS
    )
    ''')

    stat_userops = execute_sql('''
    SELECT 
    COUNT(*) as NUM_USEROPS
    FROM 
    (
    SELECT 
    'arbitrum' as chain,
    BLOCK_TIME,
    OP_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    
    UNION ALL
    SELECT 
    'ethereum' as chain,
    BLOCK_TIME,
    OP_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    
    UNION ALL
    SELECT 
    'optimism' as chain,
    BLOCK_TIME,
    OP_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    
    UNION ALL
    SELECT 
    'polygon' as chain,
    BLOCK_TIME,
    OP_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS

    UNION ALL
    SELECT 
    'base' as chain,
    BLOCK_TIME,
    OP_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS

    UNION ALL
    SELECT 
    'avalanche' as chain,
    BLOCK_TIME,
    OP_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    )
    ''')

    stat_txns = execute_sql('''
    SELECT 
    COUNT(*) as NUM_TXNS
    FROM 
    (
    SELECT 
    'arbitrum' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
    
    UNION ALL
    SELECT 
    'ethereum' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
    
    UNION ALL
    SELECT 
    'optimism' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
    
    UNION ALL
    SELECT 
    'polygon' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ENTRYPOINT_TRANSACTIONS

    UNION ALL
    SELECT 
    'base' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ENTRYPOINT_TRANSACTIONS

    UNION ALL
    SELECT 
    'avalanche' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ENTRYPOINT_TRANSACTIONS
    )
    ''')

    stat_paymaster_spend = execute_sql('''
    SELECT
    ROUND(SUM(GAS_SPENT),2) AS GAS_SPENT
    FROM (
    SELECT 
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    
    UNION ALL
    SELECT 
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    
    UNION ALL
    SELECT 
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    
    UNION ALL
    SELECT 
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000

    UNION ALL
    SELECT 
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000

    UNION ALL
    SELECT 
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000' 
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    )
    ''')

    monthly_active_accounts = execute_sql('''
    SELECT 
    TO_VARCHAR(month, 'YYYY-MM-DD') as DATE,
    chain,
    num_accounts
    FROM(
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'arbitrum' as chain,
    COUNT(DISTINCT SENDER) as num_accounts
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'ethereum' as chain,
    COUNT(DISTINCT SENDER) as num_accounts
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'optimism' as chain,
    COUNT(DISTINCT SENDER) as num_accounts
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'polygon' as chain,
    COUNT(DISTINCT SENDER) as num_accounts
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    GROUP BY 1,2

    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'base' as chain,
    COUNT(DISTINCT SENDER) as num_accounts
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    GROUP BY 1,2

    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'avalanche' as chain,
    COUNT(DISTINCT SENDER) as num_accounts
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    GROUP BY 1,2
    )
    ORDER BY 1
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_userops = execute_sql('''
    SELECT 
    TO_VARCHAR(month, 'YYYY-MM-DD') as DATE,
    chain,
    num_userops
    FROM (
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'arbitrum' as chain,
    COUNT(*) as num_userops
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'ethereum' as chain,
    COUNT(*) as num_userops
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'optimism' as chain,
    COUNT(*) as num_userops
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'polygon' as chain,
    COUNT(*) as num_userops
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    GROUP BY 1,2

    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'base' as chain,
    COUNT(*) as num_userops
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    GROUP BY 1,2

    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as month,
    'avalanche' as chain,
    COUNT(*) as num_userops
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    GROUP BY 1,2
    )
    ORDER BY 1
    ''',
                                  chain=chain,
                                  time=timeframe)

    monthly_paymaster_spend = execute_sql('''
    SELECT
    TO_VARCHAR(month, 'YYYY-MM-DD') as DATE,
    chain,
    GAS_SPENT
    FROM (
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as MONTH,
    'ethereum' as chain,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as MONTH,
    'arbitrum' as chain,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as MONTH,
    'optimism' as chain,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1,2
    
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as MONTH,
    'polygon' as chain,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1,2

    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as MONTH,
    'base' as chain,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1,2

    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as MONTH,
    'avalanche' as chain,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1,2
    )
    ORDER BY 1
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_bundler_revenue = execute_sql('''
    SELECT 
    TO_VARCHAR(TIME, 'YYYY-MM-DD') as DATE,
    chain,
    SUM(BUNDLER_REVENUE_USD) AS REVENUE
    FROM 
    (
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as TIME,
    'polygon' as chain,
    SUM(BUNDLER_REVENUE_USD) AS BUNDLER_REVENUE_USD
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    GROUP BY 1,2
    
    UNION ALL 
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as TIME,
    'optimism' as chain,
    SUM(BUNDLER_REVENUE_USD) AS BUNDLER_REVENUE_USD
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    GROUP BY 1, 2
    
    UNION ALL 
    SELECT
    date_trunc('{time}', BLOCK_TIME) as TIME, 
    'arbitrum' as chain,
    SUM(BUNDLER_REVENUE_USD) AS BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000    
    GROUP BY 1, 2
    
    UNION ALL 
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as TIME, 
    'ethereum' as chain,
    SUM(BUNDLER_REVENUE_USD) AS BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000   
    GROUP BY 1, 2

    UNION ALL 
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as TIME, 
    'base' as chain,
    SUM(BUNDLER_REVENUE_USD) AS BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000   
    GROUP BY 1, 2

    UNION ALL 
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as TIME, 
    'avalanche' as chain,
    SUM(BUNDLER_REVENUE_USD) AS BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000   
    GROUP BY 1, 2
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                          chain=chain,
                                          time=timeframe)

    if timeframe == 'week':
      retention_scope = 12
    elif timeframe == 'month':
      retention_scope = 6
    elif timeframe == 'day':
      retention_scope = 14

    retention = execute_sql('''
    WITH transactions AS (
      SELECT SENDER, BLOCK_TIME AS created_at
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      UNION ALL
      SELECT SENDER, BLOCK_TIME AS created_at
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      UNION ALL
      SELECT SENDER, BLOCK_TIME AS created_at
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      UNION ALL
      SELECT SENDER, BLOCK_TIME AS created_at
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      UNION ALL
      SELECT SENDER, BLOCK_TIME AS created_at
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      UNION ALL
      SELECT SENDER, BLOCK_TIME AS created_at
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    ),

    cohort AS (
      SELECT 
        SENDER,
        MIN(date_trunc('{time}', created_at)) AS cohort_{time}
      FROM transactions
      GROUP BY 1
    ),

    cohort_size AS (
      SELECT
        cohort_{time},
        COUNT(1) as num_users
      FROM cohort
      GROUP BY cohort_{time}
    ),

    user_activities AS (
      SELECT
        DISTINCT
          DATEDIFF({time}, cohort_{time}, created_at) AS {time}_number,
          A.SENDER
      FROM transactions AS A
      LEFT JOIN cohort AS C 
      ON A.SENDER = C.SENDER
    ),

    retention_table AS (
      SELECT
        cohort_{time},
        A.{time}_number,
        COUNT(1) AS num_users
      FROM user_activities A
      LEFT JOIN cohort AS C 
      ON A.SENDER = C.SENDER
      GROUP BY 1, 2  
    )

    SELECT
      TO_VARCHAR(date_trunc('{time}', A.cohort_{time}), 'YYYY-MM-DD') AS cohort,
      B.num_users AS total_users,
      A.{time}_number,
      ROUND((A.num_users * 100 / B.num_users), 2) as percentage
    FROM retention_table AS A
    LEFT JOIN cohort_size AS B
    ON A.cohort_{time} = B.cohort_{time}
    WHERE 
      A.cohort_{time} IS NOT NULL
      AND A.cohort_{time} >= date_trunc('{time}', (CURRENT_TIMESTAMP() - interval '{retention_scope} {time}'))  
      AND A.cohort_{time} < date_trunc('{time}', CURRENT_TIMESTAMP())
    ORDER BY 1, 3
    ''',
                            chain=chain,
                            time=timeframe,
                            retention_scope=retention_scope)

    userops_by_type = execute_sql('''
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    CATEGORY,
    SUM(NUM_OPS) AS NUM_OPS
    FROM (
    SELECT 
    DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
    CASE WHEN u.CALLED_CONTRACT = 'direct_transfer' THEN 'native transfer'
    ELSE COALESCE(l.CATEGORY, 'unlabeled') 
    END AS CATEGORY,
    COUNT(*) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1, 2
    
    UNION ALL SELECT 
    DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
    CASE WHEN u.CALLED_CONTRACT = 'direct_transfer' THEN 'native transfer'
    ELSE COALESCE(l.CATEGORY, 'unlabeled') 
    END AS CATEGORY,
    COUNT(*) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1, 2
    
    UNION ALL SELECT 
    DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
    CASE WHEN u.CALLED_CONTRACT = 'direct_transfer' THEN 'native transfer'
    ELSE COALESCE(l.CATEGORY, 'unlabeled') 
    END AS CATEGORY,
    COUNT(*) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1, 2
    
    UNION ALL SELECT 
    DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
    CASE WHEN u.CALLED_CONTRACT = 'direct_transfer' THEN 'native transfer'
    ELSE COALESCE(l.CATEGORY, 'unlabeled') 
    END AS CATEGORY,
    COUNT(*) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1, 2
    
    UNION ALL SELECT 
    DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
    CASE WHEN u.CALLED_CONTRACT = 'direct_transfer' THEN 'native transfer'
    ELSE COALESCE(l.CATEGORY, 'unlabeled') 
    END AS CATEGORY,
    COUNT(*) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1, 2
    
    UNION ALL SELECT 
    DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
    CASE WHEN u.CALLED_CONTRACT = 'direct_transfer' THEN 'native transfer'
    ELSE COALESCE(l.CATEGORY, 'unlabeled') 
    END AS CATEGORY,
    COUNT(*) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1, 2
    )
    GROUP BY 1, 2
    ORDER BY 1
    ''',
                                  time=timeframe)

    accounts_by_category = execute_sql('''
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
    SELECT 
    date_trunc('{time}', BLOCK_TIME) AS DATE,
    SENDER,
    count(OP_HASH) AS NUM_OPS
    FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_POLYGON_USEROPS"
    GROUP BY 1,2
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) AS DATE,
    SENDER,
    count(OP_HASH) AS NUM_OPS
    FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_BASE_USEROPS"
    GROUP BY 1,2
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) AS DATE,
    SENDER,
    count(OP_HASH) AS NUM_OPS
    FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_OPTIMISM_USEROPS"
    GROUP BY 1,2
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) AS DATE,
    SENDER,
    count(OP_HASH) AS NUM_OPS
    FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ARBITRUM_USEROPS"
    GROUP BY 1,2
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) AS DATE,
    SENDER,
    count(OP_HASH) AS NUM_OPS
    FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ETHEREUM_USEROPS"
    GROUP BY 1,2
    UNION ALL
    SELECT 
    date_trunc('{time}', BLOCK_TIME) AS DATE,
    SENDER,
    count(OP_HASH) AS NUM_OPS
    FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_AVALANCHE_USEROPS"
    GROUP BY 1,2
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                       time=timeframe)

    response_data = {
      "deployments": stat_deployments,
      "userops": stat_userops,
      "transactions": stat_txns,
      "paymaster_spend": stat_paymaster_spend,
      "monthly_active_accounts": monthly_active_accounts,
      "monthly_userops": monthly_userops,
      "monthly_paymaster_spend": monthly_paymaster_spend,
      "monthly_bundler_revenue": monthly_bundler_revenue,
      "retention": retention,
      "userops_by_type": userops_by_type,
      "accounts_by_category": accounts_by_category
    }

    return jsonify(response_data)

  else:
    stat_deployments = execute_sql('''
    SELECT COUNT(*) as NUM_DEPLOYMENTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS
    ''',
                                   chain=chain)

    stat_userops = execute_sql('''
    SELECT COUNT(*) as NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    ''',
                               chain=chain)

    stat_txns = execute_sql('''
    SELECT COUNT(*) as NUM_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    ''',
                            chain=chain)

    stat_paymaster_spend = execute_sql('''
    SELECT 
    ROUND(SUM(ACTUALGASCOST_USD),2) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    ''',
                                       chain=chain)

    monthly_active_accounts = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    COUNT(DISTINCT SENDER) as NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1
    ORDER BY 1
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_userops = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    COUNT(*) as NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1
    ORDER BY 1
    ''',
                                  chain=chain,
                                  time=timeframe)

    monthly_paymaster_spend = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1
    ORDER BY 1
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_bundler_revenue = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    SUM(BUNDLER_REVENUE_USD) AS REVENUE
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    GROUP BY 1
    ORDER BY 1
    ''',
                                          chain=chain,
                                          time=timeframe)

    if timeframe == 'week':
      retention_scope = 12
    elif timeframe == 'month':
      retention_scope = 6
    elif timeframe == 'day':
      retention_scope = 14

    retention = execute_sql('''
    WITH transactions AS (
      SELECT SENDER, BLOCK_TIME AS created_at
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    ),

    cohort AS (
      SELECT 
        SENDER,
        MIN(date_trunc('{time}', created_at)) AS cohort_{time}
      FROM transactions
      GROUP BY 1
    ),

    cohort_size AS (
      SELECT
        cohort_{time},
        COUNT(1) as num_users
      FROM cohort
      GROUP BY cohort_{time}
    ),

    user_activities AS (
      SELECT
        DISTINCT
          DATEDIFF({time}, cohort_{time}, created_at) AS {time}_number,
          A.SENDER
      FROM transactions AS A
      LEFT JOIN cohort AS C 
      ON A.SENDER = C.SENDER
    ),

    retention_table AS (
      SELECT
        cohort_{time},
        A.{time}_number,
        COUNT(1) AS num_users
      FROM user_activities A
      LEFT JOIN cohort AS C 
      ON A.SENDER = C.SENDER
      GROUP BY 1, 2  
    )

    SELECT
      TO_VARCHAR(date_trunc('{time}', A.cohort_{time}), 'YYYY-MM-DD') AS cohort,
      B.num_users AS total_users,
      A.{time}_number,
      ROUND((A.num_users * 100 / B.num_users), 2) as percentage
    FROM retention_table AS A
    LEFT JOIN cohort_size AS B
    ON A.cohort_{time} = B.cohort_{time}
    WHERE 
      A.cohort_{time} IS NOT NULL
      AND A.cohort_{time} >= date_trunc('{time}', (CURRENT_TIMESTAMP() - interval '{retention_scope} {time}'))  
      AND A.cohort_{time} < date_trunc('{time}', CURRENT_TIMESTAMP())
    ORDER BY 1, 3
    ''',
                            chain=chain,
                            time=timeframe,
                            retention_scope=retention_scope)

    userops_by_type = execute_sql('''
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    CATEGORY,
    SUM(NUM_OPS) AS NUM_OPS
    FROM (
    SELECT 
    DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
    CASE WHEN u.CALLED_CONTRACT = 'direct_transfer' THEN 'native transfer'
    ELSE COALESCE(l.CATEGORY, 'unlabeled') 
    END AS CATEGORY,
    COUNT(*) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1, 2
    )
    GROUP BY 1, 2
    ORDER BY 1
    ''',
                                  chain=chain,
                                  time=timeframe)

    accounts_by_category = execute_sql('''
    SELECT 
    TO_VARCHAR(DATE, 'YYYY-MM-DD') AS DATE,
    CASE WHEN NUM_OPS = 1 THEN '01 UserOp'
    WHEN NUM_OPS > 1 AND NUM_OPS <= 10 THEN '02-10 UserOps'
    ELSE 'More than 10 UserOps'
    END AS CATEGORY,
    COUNT(SENDER) AS NUM_ACCOUNTS
    FROM (
    SELECT 
    date_trunc('{time}', BLOCK_TIME) AS DATE,
    SENDER,
    count(OP_HASH) AS NUM_OPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                       chain=chain,
                                       time=timeframe)

    response_data = {
      "deployments": stat_deployments,
      "userops": stat_userops,
      "transactions": stat_txns,
      "paymaster_spend": stat_paymaster_spend,
      "monthly_active_accounts": monthly_active_accounts,
      "monthly_userops": monthly_userops,
      "monthly_paymaster_spend": monthly_paymaster_spend,
      "monthly_bundler_revenue": monthly_bundler_revenue,
      "retention": retention,
      "userops_by_type": userops_by_type,
      "accounts_by_category": accounts_by_category
    }

    return jsonify(response_data)


@app.route('/bundler')
@cache.memoize(make_name=make_cache_key)
def bundler():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if chain == 'all':
    leaderboard = execute_sql('''
    WITH txns AS (
    SELECT 
    BUNDLER_NAME,
    COUNT(*) AS NUM_TXNS,
    SUM(BUNDLER_REVENUE_USD) AS REVENUE
    FROM 
    (
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    )
    GROUP BY 1
    ),
    
    usops AS (
    SELECT 
    BUNDLER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM
    (
    SELECT BUNDLER_NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    UNION ALL 
    SELECT BUNDLER_NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    UNION ALL 
    SELECT BUNDLER_NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    UNION ALL 
    SELECT BUNDLER_NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    UNION ALL 
    SELECT BUNDLER_NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    UNION ALL 
    SELECT BUNDLER_NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    )
    GROUP BY 1
    )
    
    SELECT 
    t.BUNDLER_NAME,
    NUM_USEROPS,
    NUM_TXNS,
    REVENUE
    FROM txns t
    INNER JOIN usops u ON u.BUNDLER_NAME = t.BUNDLER_NAME
    ORDER BY 2 DESC
    ''')

    userops_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM (
    SELECT BLOCK_TIME, BUNDLER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, BUNDLER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, BUNDLER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, BUNDLER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, BUNDLER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, BUNDLER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                time=timeframe)

    revenue_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    SUM(BUNDLER_REVENUE_USD) AS REVENUE
    FROM 
    (
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000
    UNION ALL
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000
    UNION ALL
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                time=timeframe)

    multi_userop_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        100*COALESCE(SUM(CASE WHEN NUM_USEROPS > 1 THEN 1 ELSE 0 END) / COUNT(*), 0) as pct_multi_userop
    FROM (
        SELECT BLOCK_TIME, NUM_USEROPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ENTRYPOINT_TRANSACTIONS
        UNION ALL 
        SELECT BLOCK_TIME, NUM_USEROPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
        UNION ALL 
        SELECT BLOCK_TIME, NUM_USEROPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
        UNION ALL 
        SELECT BLOCK_TIME, NUM_USEROPS 
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
        UNION ALL 
        SELECT BLOCK_TIME, NUM_USEROPS 
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ENTRYPOINT_TRANSACTIONS
        UNION ALL 
        SELECT BLOCK_TIME, NUM_USEROPS 
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ENTRYPOINT_TRANSACTIONS
    )
    GROUP BY 1
    ORDER BY 1
    ''',
                                     time=timeframe)

    accounts_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM (
    SELECT BLOCK_TIME, SENDER, BUNDLER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, BUNDLER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, BUNDLER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, BUNDLER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, BUNDLER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, BUNDLER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 time=timeframe)

    frontrun_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES
    FROM (
    SELECT BLOCK_TIME, BUNDLER_NAME, tx_hash
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, BUNDLER_NAME , tx_hash
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, BUNDLER_NAME , tx_hash
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, BUNDLER_NAME , tx_hash
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, BUNDLER_NAME , tx_hash
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, BUNDLER_NAME , tx_hash
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_FAILED_VALIDATION_OPS
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 time=timeframe)

    frontrun_pct_chart = execute_sql('''
    WITH failed_ops AS (    
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as DATE,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_FAILED
    FROM (
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_FAILED_VALIDATION_OPS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_FAILED_VALIDATION_OPS
    )
    GROUP BY 1
    ),
    
    all_ops AS (
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as DATE,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_ALL
    FROM (
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ENTRYPOINT_TRANSACTIONS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ENTRYPOINT_TRANSACTIONS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
    UNION ALL
    SELECT BLOCK_TIME, tx_hash 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ENTRYPOINT_TRANSACTIONS
    )
    GROUP BY 1
    )
    
    SELECT
        TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    100 * NUM_BUNDLES_FAILED/NUM_BUNDLES_ALL AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    ORDER BY 1
    ''',
                                     time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "revenue_chart": revenue_chart,
      "multi_userop_chart": multi_userop_chart,
      "accounts_chart": accounts_chart,
      "frontrun_chart": frontrun_chart,
      "frontrun_pct_chart": frontrun_pct_chart
    }

    return jsonify(response_data)

  else:
    leaderboard = execute_sql('''
    WITH txns AS (
    SELECT 
    BUNDLER_NAME,
    COUNT(*) AS NUM_TXNS,
    SUM(BUNDLER_REVENUE_USD) AS REVENUE
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    GROUP BY 1
    ),
    
    usops AS (
    SELECT 
    BUNDLER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1
    )
    
    SELECT 
    t.BUNDLER_NAME,
    NUM_USEROPS,
    NUM_TXNS,
    REVENUE
    FROM txns t
    INNER JOIN usops u ON u.BUNDLER_NAME = t.BUNDLER_NAME
    ORDER BY 2 DESC
    ''',
                              chain=chain)

    userops_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                chain=chain,
                                time=timeframe)

    revenue_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    SUM(BUNDLER_REVENUE_USD) AS REVENUE
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000000
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                chain=chain,
                                time=timeframe)

    multi_userop_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    100*COALESCE(SUM(CASE WHEN NUM_USEROPS > 1 THEN 1 ELSE 0 END) / COUNT(*), 0) as pct_multi_userop
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    GROUP BY 1
    ORDER BY 1
    ''',
                                     chain=chain,
                                     time=timeframe)

    accounts_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 chain=chain,
                                 time=timeframe)

    frontrun_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_FAILED_VALIDATION_OPS
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 chain=chain,
                                 time=timeframe)

    frontrun_pct_chart = execute_sql('''
    WITH failed_ops AS (    
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as DATE,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_FAILED
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_FAILED_VALIDATION_OPS
    GROUP BY 1
    ),
    
    all_ops AS (
    SELECT 
    date_trunc('{time}', BLOCK_TIME) as DATE,
    COUNT(DISTINCT tx_hash) as NUM_BUNDLES_ALL
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    GROUP BY 1
    )
    
    SELECT
    TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    100 * NUM_BUNDLES_FAILED/NUM_BUNDLES_ALL  AS PCT_FRONTRUN
    FROM all_ops a
    INNER JOIN failed_ops f 
    ON a.DATE = f.DATE
    ORDER BY 1
    ''',
                                     chain=chain,
                                     time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "revenue_chart": revenue_chart,
      "multi_userop_chart": multi_userop_chart,
      "accounts_chart": accounts_chart,
      "frontrun_chart": frontrun_chart,
      "frontrun_pct_chart": frontrun_pct_chart
    }

    return jsonify(response_data)


@app.route('/paymaster')
@cache.memoize(make_name=make_cache_key)
def paymaster():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if chain == 'all':
    leaderboard = execute_sql('''
    SELECT 
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM
    (
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    )
    GROUP BY 1
    ORDER BY 3 DESC
    ''')

    userops_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM (
    SELECT BLOCK_TIME, PAYMASTER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                time=timeframe)

    spend_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM
    (
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000' 
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    )
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                              time=timeframe)

    accounts_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM (
    SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 time=timeframe)

    spend_type_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM
    (
    SELECT BLOCK_TIME, PAYMASTER_TYPE, ACTUALGASCOST_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000' 
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    )
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                                   time=timeframe)

    userops_type_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    COUNT(*) AS NUM_USEROPS
    FROM (
    SELECT BLOCK_TIME, PAYMASTER_TYPE 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_TYPE 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                     time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "spend_chart": spend_chart,
      "accounts_chart": accounts_chart,
      "spend_type_chart": spend_type_chart,
      "userops_type_chart": userops_type_chart
    }

    return jsonify(response_data)

  else:
    leaderboard = execute_sql('''
    SELECT 
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1
    ORDER BY 3 DESC
    ''',
                              chain=chain)

    userops_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM  BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                chain=chain,
                                time=timeframe)

    spend_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                              chain=chain,
                              time=timeframe)

    accounts_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 chain=chain,
                                 time=timeframe)

    spend_type_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                                   chain=chain,
                                   time=timeframe)

    userops_type_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
         WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
         WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
         ELSE PAYMASTER_TYPE
    END AS PAYMASTER_TYPE,
    COUNT(*) AS NUM_USEROPS
    FROM  BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                     chain=chain,
                                     time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "spend_chart": spend_chart,
      "accounts_chart": accounts_chart,
      "spend_type_chart": spend_type_chart,
      "userops_type_chart": userops_type_chart,
    }

    return jsonify(response_data)


@app.route('/account_deployer')
@cache.memoize(make_name=make_cache_key)
def account_deployer():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if chain == 'all':
    leaderboard = execute_sql('''
    SELECT 
    FACTORY_NAME AS DEPLOYER_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM (
    SELECT FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ACCOUNT_DEPLOYMENTS
    )
    GROUP BY 1
    ORDER BY 2 DESC
    ''')

    deployments_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    FACTORY_NAME AS DEPLOYER_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM (
    SELECT BLOCK_TIME, FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT BLOCK_TIME, FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT BLOCK_TIME, FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT BLOCK_TIME, FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT BLOCK_TIME, FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ACCOUNT_DEPLOYMENTS
    UNION ALL
    SELECT BLOCK_TIME, FACTORY_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ACCOUNT_DEPLOYMENTS
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                    time=timeframe)

    accounts_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        FACTORY_NAME,
        COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'Unknown') AS FACTORY_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ARBITRUM_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ARBITRUM_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'Unknown') AS FACTORY_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ETHEREUM_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'Unknown') AS FACTORY_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_POLYGON_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_POLYGON_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'Unknown') AS FACTORY_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_OPTIMISM_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_OPTIMISM_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'Unknown') AS FACTORY_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_BASE_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_BASE_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'Unknown') AS FACTORY_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_AVALANCHE_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_AVALANCHE_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
            ON l.ADDRESS = ad.FACTORY
    ) AS combined_data
    GROUP BY 1, 2
    ORDER BY 1, 2;
    ''',
                                 time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "deployments_chart": deployments_chart,
      "accounts_chart": accounts_chart
    }

    return jsonify(response_data)

  else:
    leaderboard = execute_sql('''
    SELECT 
    FACTORY_NAME AS DEPLOYER_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS
    GROUP BY 1
    ORDER BY 2 DESC
    ''',
                              chain=chain)

    deployments_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    FACTORY_NAME AS DEPLOYER_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                    chain=chain,
                                    time=timeframe)

    accounts_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', u.BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        COALESCE(l.name, 'Unknown') AS FACTORY_NAME, 
        COUNT(DISTINCT u.SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
    INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS ad
        ON ad.ACCOUNT_ADDRESS = u.SENDER
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
        ON l.ADDRESS = ad.FACTORY
    GROUP BY 1, 2
    ORDER BY 1, 2
    ''',
                                 chain=chain,
                                 time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "deployments_chart": deployments_chart,
      "accounts_chart": accounts_chart
    }

    return jsonify(response_data)


@app.route('/apps')
@cache.memoize(make_name=make_cache_key)
def apps():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if chain == 'all':
    usage_chart = execute_sql('''
    WITH CombinedUserOps AS (
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    ),
    RankedProjects AS (
      SELECT 
        DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIME) ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
      FROM 
        CombinedUserOps u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
      GROUP BY 
        1, 2
    ),
    GroupedProjects AS (
      SELECT 
        DATE, 
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
      FROM 
        RankedProjects
      GROUP BY 
        1, 2
    )
    SELECT 
      TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, PROJECT, NUM_UNIQUE_SENDERS
    FROM 
      GroupedProjects
    ORDER BY 
      DATE DESC, NUM_UNIQUE_SENDERS DESC;
    ''',
                              time=timeframe)

    leaderboard = execute_sql('''
    WITH CombinedUserOps AS (
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER, OP_HASH FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER, OP_HASH FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER, OP_HASH FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER, OP_HASH FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER, OP_HASH FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      UNION ALL
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER, OP_HASH FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
    ),
    RankedProjects AS (
      SELECT 
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        COUNT(DISTINCT u.OP_HASH) AS NUM_OPS,
        ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
      FROM 
        CombinedUserOps u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
      GROUP BY 
        1
    ),
    GroupedProjects AS (
      SELECT 
        PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS,
        SUM(NUM_OPS) AS NUM_OPS
      FROM 
        RankedProjects
      WHERE RN <= 10
      GROUP BY 
        1
    )
    SELECT 
      PROJECT, NUM_UNIQUE_SENDERS, NUM_OPS
    FROM 
      GroupedProjects
    ORDER BY 
      NUM_UNIQUE_SENDERS DESC;
    ''')

    response_data = {"usage_chart": usage_chart, "leaderboard": leaderboard}

    return jsonify(response_data)

  else:
    usage_chart = execute_sql('''
    WITH RankedProjects AS (
      SELECT 
        DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIME) ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
      FROM 
        BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
      GROUP BY 
        1, 2
    ),
    GroupedProjects AS (
      SELECT 
        DATE, 
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS
      FROM 
        RankedProjects
      GROUP BY 
        1, 2
    )
    SELECT 
      TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, PROJECT, NUM_UNIQUE_SENDERS
    FROM 
      GroupedProjects
    ORDER BY 
      DATE DESC, NUM_UNIQUE_SENDERS DESC;
    ''',
                              chain=chain,
                              time=timeframe)

    leaderboard = execute_sql('''
    WITH RankedProjects AS (
      SELECT 
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
        COUNT(DISTINCT u.OP_HASH) AS NUM_OPS,
        ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
      FROM 
        BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
      GROUP BY 
        1
    ),
    GroupedProjects AS (
      SELECT 
        PROJECT,
        SUM(NUM_UNIQUE_SENDERS) AS NUM_UNIQUE_SENDERS,
        SUM(NUM_OPS) AS NUM_OPS
      FROM 
        RankedProjects
      WHERE RN <= 10
      GROUP BY 
        1
    )
    SELECT 
      PROJECT, NUM_UNIQUE_SENDERS, NUM_OPS
    FROM 
      GroupedProjects
    ORDER BY 
      NUM_UNIQUE_SENDERS DESC;
    ''',
                              chain=chain)

    response_data = {"usage_chart": usage_chart, "leaderboard": leaderboard}

    return jsonify(response_data)


@app.route('/wallet')
@cache.memoize(make_name=make_cache_key)
def wallet():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if chain == 'all':
    deployments_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    WALLET_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM (
    SELECT ad.BLOCK_TIME, COALESCE(l.name, 'other') AS WALLET_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ACCOUNT_DEPLOYMENTS ad
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
    ON l.address = ad.factory
    
    UNION ALL
    SELECT ad.BLOCK_TIME, COALESCE(l.name, 'other') AS WALLET_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ACCOUNT_DEPLOYMENTS ad
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
    ON l.address = ad.factory
    
    UNION ALL
    SELECT ad.BLOCK_TIME, COALESCE(l.name, 'other') AS WALLET_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ACCOUNT_DEPLOYMENTS ad
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
    ON l.address = ad.factory
    
    UNION ALL
    SELECT ad.BLOCK_TIME, COALESCE(l.name, 'other') AS WALLET_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS ad
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
    ON l.address = ad.factory
    
    UNION ALL
    SELECT ad.BLOCK_TIME, COALESCE(l.name, 'other') AS WALLET_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ACCOUNT_DEPLOYMENTS ad
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
    ON l.address = ad.factory

    UNION ALL
    SELECT ad.BLOCK_TIME, COALESCE(l.name, 'other') AS WALLET_NAME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ACCOUNT_DEPLOYMENTS ad
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
    ON l.address = ad.factory
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                    time=timeframe)

    userops_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        WALLET_NAME,
        COUNT(OP_HASH) AS NUM_USEROPS
    FROM (
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.OP_HASH
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS u
        INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ACCOUNT_DEPLOYMENTS ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.OP_HASH
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS u
        INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.OP_HASH
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS u
        INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ACCOUNT_DEPLOYMENTS ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.OP_HASH
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS u
        INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ACCOUNT_DEPLOYMENTS ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.OP_HASH
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_BASE_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_BASE_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.OP_HASH
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_AVALANCHE_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_AVALANCHE_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY
    ) AS combined_data
    GROUP BY 1, 2
    ORDER BY 1, 2;
    ''',
                                time=timeframe)

    accounts_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        WALLET_NAME,
        COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM (
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ARBITRUM_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ARBITRUM_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ETHEREUM_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_ETHEREUM_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_POLYGON_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_POLYGON_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_OPTIMISM_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_OPTIMISM_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_BASE_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_BASE_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY

        UNION ALL
        SELECT
            u.BLOCK_TIME,
            COALESCE(l.name, 'other') AS WALLET_NAME, 
            u.SENDER
        FROM "BUNDLEBEAR"."DBT_KOFI"."ERC4337_AVALANCHE_USEROPS" u
        INNER JOIN "BUNDLEBEAR"."DBT_KOFI"."ERC4337_AVALANCHE_ACCOUNT_DEPLOYMENTS" ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
            ON l.ADDRESS = ad.FACTORY
    ) AS combined_data
    GROUP BY 1, 2
    ORDER BY 1, 2;
    ''',
                                 time=timeframe)

    response_data = {
      "deployments_chart": deployments_chart,
      "userops_chart": userops_chart,
      "accounts_chart": accounts_chart
    }

    return jsonify(response_data)

  else:

    deployments_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', ad.BLOCK_TIME),'YYYY-MM-DD') as DATE,
    COALESCE(l.name, 'other') AS WALLET_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS ad
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
        ON l.address = ad.factory
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                    chain=chain,
                                    time=timeframe)

    userops_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', u.BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        COALESCE(l.name, 'other') AS WALLET_NAME, 
        COUNT(u.OP_HASH) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
    INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS ad
        ON ad.ACCOUNT_ADDRESS = u.SENDER
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
        ON l.ADDRESS = ad.FACTORY
    GROUP BY 1, 2
    ORDER BY 1, 2
    ''',
                                chain=chain,
                                time=timeframe)

    accounts_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', u.BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        COALESCE(l.name, 'other') AS WALLET_NAME, 
        COUNT(DISTINCT u.SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
    INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS ad
        ON ad.ACCOUNT_ADDRESS = u.SENDER
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_WALLETS l
        ON l.ADDRESS = ad.FACTORY
    GROUP BY 1, 2
    ORDER BY 1, 2
    ''',
                                 chain=chain,
                                 time=timeframe)

    response_data = {
      "deployments_chart": deployments_chart,
      "userops_chart": userops_chart,
      "accounts_chart": accounts_chart
    }

    return jsonify(response_data)


@app.route('/entity')
@cache.memoize(make_name=make_cache_key)
def entity():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')
  entity = request.args.get('entity', 'pimlico')

  entity_type = execute_sql('''
  SELECT
  CASE 
      WHEN '{entity}' IN (SELECT NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_BUNDLERS) THEN true
      ELSE false
  END AS bundler_exists,
  CASE 
      WHEN '{entity}' IN (SELECT NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_PAYMASTERS) THEN true
      ELSE false
  END AS paymaster_exists
  ''',
                            entity=entity)
  bundler_exists = entity_type[0]['BUNDLER_EXISTS']
  paymaster_exists = entity_type[0]['PAYMASTER_EXISTS']

  if chain == 'all':
    if bundler_exists:
      bundler_userops_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(*) AS NUM_USEROPS
      FROM (
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      )
      GROUP BY 1
      ORDER BY 1
      ''',
                                          time=timeframe,
                                          entity=entity)

      bundler_accounts_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
      FROM (
      SELECT BLOCK_TIME, SENDER
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      )
      GROUP BY 1
      ORDER BY 1
      ''',
                                           time=timeframe,
                                           entity=entity)

      bundler_revenue_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      SUM(BUNDLER_REVENUE_USD) AS REVENUE
      FROM 
      (
      SELECT BUNDLER_REVENUE_USD, BLOCK_TIME 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_ENTRYPOINT_TRANSACTIONS
      WHERE BUNDLER_REVENUE_USD != 'NaN'
      AND BUNDLER_REVENUE_USD < 1000000
      AND BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BUNDLER_REVENUE_USD, BLOCK_TIME 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
      WHERE BUNDLER_REVENUE_USD != 'NaN'
      AND BUNDLER_REVENUE_USD < 1000000
      AND BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BUNDLER_REVENUE_USD, BLOCK_TIME 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
      WHERE BUNDLER_REVENUE_USD != 'NaN'
      AND BUNDLER_REVENUE_USD < 1000000
      AND BUNDLER_NAME = '{entity}'
      UNION ALL 
      SELECT BUNDLER_REVENUE_USD, BLOCK_TIME 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
      WHERE BUNDLER_REVENUE_USD != 'NaN'
      AND BUNDLER_REVENUE_USD < 1000000
      AND BUNDLER_NAME = '{entity}'
      UNION ALL
      SELECT BUNDLER_REVENUE_USD, BLOCK_TIME 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_ENTRYPOINT_TRANSACTIONS
      WHERE BUNDLER_REVENUE_USD != 'NaN'
      AND BUNDLER_REVENUE_USD < 1000000
      AND BUNDLER_NAME = '{entity}'
      UNION ALL
      SELECT BUNDLER_REVENUE_USD, BLOCK_TIME 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_ENTRYPOINT_TRANSACTIONS
      WHERE BUNDLER_REVENUE_USD != 'NaN'
      AND BUNDLER_REVENUE_USD < 1000000
      AND BUNDLER_NAME = '{entity}'
      )
      GROUP BY 1
      ORDER BY 1
      ''',
                                          time=timeframe,
                                          entity=entity)

    else:
      bundler_userops_chart = '0'
      bundler_accounts_chart = '0'
      bundler_revenue_chart = '0'

    if paymaster_exists:
      paymaster_userops_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(*) AS NUM_USEROPS
      FROM (
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      )
      GROUP BY 1
      ORDER BY 1
      ''',
                                            time=timeframe,
                                            entity=entity)

      paymaster_spend_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      SUM(ACTUALGASCOST_USD) AS GAS_SPENT
      FROM
      (
      SELECT BLOCK_TIME, ACTUALGASCOST_USD 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND ACTUALGASCOST_USD != 'NaN'
      AND ACTUALGASCOST_USD < 1000000000
      AND PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, ACTUALGASCOST_USD  
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND ACTUALGASCOST_USD != 'NaN'
      AND ACTUALGASCOST_USD < 1000000000
      AND PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, ACTUALGASCOST_USD  
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND ACTUALGASCOST_USD != 'NaN'
      AND ACTUALGASCOST_USD < 1000000000
      AND PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, ACTUALGASCOST_USD  
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND ACTUALGASCOST_USD != 'NaN'
      AND ACTUALGASCOST_USD < 1000000000
      AND PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, ACTUALGASCOST_USD  
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND ACTUALGASCOST_USD != 'NaN'
      AND ACTUALGASCOST_USD < 1000000000
      AND PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, ACTUALGASCOST_USD  
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND ACTUALGASCOST_USD != 'NaN'
      AND ACTUALGASCOST_USD < 1000000000
      AND PAYMASTER_NAME = '{entity}'
      )
      GROUP BY 1
      ORDER BY 1
      ''',
                                          time=timeframe,
                                          entity=entity)

      paymaster_accounts_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
      FROM (
      SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME 
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      UNION ALL 
      SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BASE_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
       UNION ALL 
       SELECT BLOCK_TIME, SENDER, PAYMASTER_NAME
       FROM BUNDLEBEAR.DBT_KOFI.ERC4337_AVALANCHE_USEROPS
       WHERE PAYMASTER_NAME = '{entity}'
      )
      GROUP BY 1
      ORDER BY 1
      ''',
                                             time=timeframe,
                                             entity=entity)

    else:
      paymaster_userops_chart = '0'
      paymaster_spend_chart = '0'
      paymaster_accounts_chart = '0'

    response_data = {
      "bundler_exists": bundler_exists,
      "bundler_userops_chart": bundler_userops_chart,
      "bundler_accounts_chart": bundler_accounts_chart,
      "bundler_revenue_chart": bundler_revenue_chart,
      "paymaster_exists": paymaster_exists,
      "paymaster_userops_chart": paymaster_userops_chart,
      "paymaster_spend_chart": paymaster_spend_chart,
      "paymaster_accounts_chart": paymaster_accounts_chart
    }

    return jsonify(response_data)

  else:
    if bundler_exists:
      bundler_userops_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(*) AS NUM_USEROPS
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      GROUP BY 1
      ORDER BY 1
      ''',
                                          time=timeframe,
                                          chain=chain,
                                          entity=entity)

      bundler_accounts_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
      WHERE BUNDLER_NAME = '{entity}'
      GROUP BY 1
      ORDER BY 1
      ''',
                                           time=timeframe,
                                           chain=chain,
                                           entity=entity)

      bundler_revenue_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      SUM(BUNDLER_REVENUE_USD) AS REVENUE
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
      WHERE BUNDLER_REVENUE_USD != 'NaN'
      AND BUNDLER_REVENUE_USD < 1000000000
      AND BUNDLER_NAME = '{entity}'
      GROUP BY 1
      ORDER BY 1
      ''',
                                          time=timeframe,
                                          chain=chain,
                                          entity=entity)

    else:
      bundler_userops_chart = '0'
      bundler_accounts_chart = '0'
      bundler_revenue_chart = '0'

    if paymaster_exists:
      paymaster_userops_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(*) AS NUM_USEROPS
      FROM  BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      GROUP BY 1
      ORDER BY 1
      ''',
                                            time=timeframe,
                                            chain=chain,
                                            entity=entity)

      paymaster_spend_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      SUM(ACTUALGASCOST_USD) AS GAS_SPENT
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND ACTUALGASCOST_USD != 'NaN'
      AND ACTUALGASCOST_USD < 1000000000
      AND PAYMASTER_NAME = '{entity}'
      GROUP BY 1
      ORDER BY 1 
      ''',
                                          time=timeframe,
                                          chain=chain,
                                          entity=entity)

      paymaster_accounts_chart = execute_sql('''
      SELECT 
      TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
      COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
      FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
      WHERE PAYMASTER_NAME = '{entity}'
      GROUP BY 1
      ORDER BY 1
      ''',
                                             time=timeframe,
                                             chain=chain,
                                             entity=entity)

    else:
      paymaster_userops_chart = '0'
      paymaster_spend_chart = '0'
      paymaster_accounts_chart = '0'

    response_data = {
      "bundler_exists": bundler_exists,
      "bundler_userops_chart": bundler_userops_chart,
      "bundler_accounts_chart": bundler_accounts_chart,
      "bundler_revenue_chart": bundler_revenue_chart,
      "paymaster_exists": paymaster_exists,
      "paymaster_userops_chart": paymaster_userops_chart,
      "paymaster_spend_chart": paymaster_spend_chart,
      "paymaster_accounts_chart": paymaster_accounts_chart
    }

    return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)

# REQUIREMENTS:
# 1. TO GET SNOWFLAKE
# POETRY ADD snowflake-connector-python
# 2. TO GET SSL
# sed -i '/    ];/i\      pkgs.openssl.out' replit.nix
