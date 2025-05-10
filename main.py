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
  "CACHE_DEFAULT_TIMEOUT": 21600,
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
    summary_stats = execute_sql('''
    WITH deployments AS (
    SELECT COUNT(*) as NUM_DEPLOYMENTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_DEPLOYMENTS
    )
    
    , userops AS (
        SELECT COUNT(*) as NUM_USEROPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    )
    
    , txns AS (
        SELECT COUNT(*) as NUM_TXNS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
    )
    
    , paymaster_spend AS (
        SELECT 
        ROUND(SUM(ACTUALGASCOST_USD)) AS GAS_SPENT
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
        WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
        AND ACTUALGASCOST_USD != 'NaN'
        AND ACTUALGASCOST_USD < 1000000000
    )
    
    SELECT * FROM deployments, userops, txns, paymaster_spend
    ''')

    stat_deployments = [{
      "NUM_DEPLOYMENTS": summary_stats[0]["NUM_DEPLOYMENTS"]
    }]

    stat_userops = [{"NUM_USEROPS": summary_stats[0]["NUM_USEROPS"]}]

    stat_txns = [{"NUM_TXNS": summary_stats[0]["NUM_TXNS"]}]

    stat_paymaster_spend = [{"GAS_SPENT": summary_stats[0]["GAS_SPENT"]}]

    monthly_active_accounts = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    chain,
    COUNT(DISTINCT SENDER) as num_accounts
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                          time=timeframe)

    monthly_userops = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    CHAIN,
    COUNT(OP_HASH) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                                  time=timeframe)

    monthly_paymaster_spend = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', DATE), 'YYYY-MM-DD') as DATE,
    CHAIN,
    SUM(GAS_SPENT) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_DAY_PAYMASTER_SPEND_CHAIN
    WHERE DATE > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                                          time=timeframe)

    monthly_bundler_revenue = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', DATE), 'YYYY-MM-DD') as DATE,
    CHAIN,
    SUM(REVENUE) AS REVENUE
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_DAY_BUNDLER_REVENUE_CHAIN
    WHERE DATE > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                                          time=timeframe)

    retention = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_{time}_RETENTION
    ORDER BY 1, 3
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
      # "userops_by_type": userops_by_type,
      "accounts_by_category": accounts_by_category
    }

    return jsonify(response_data)

  else:
    summary_stats = execute_sql('''
    WITH deployments AS (
    SELECT COUNT(*) as NUM_DEPLOYMENTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS
    )

    , userops AS (
        SELECT COUNT(*) as NUM_USEROPS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    )

    , txns AS (
        SELECT COUNT(*) as NUM_TXNS
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    )

    , paymaster_spend AS (
        SELECT 
        ROUND(SUM(ACTUALGASCOST_USD)) AS GAS_SPENT
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
        WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
        AND ACTUALGASCOST_USD != 'NaN'
        AND ACTUALGASCOST_USD < 1000000000
    )

    SELECT * FROM deployments, userops, txns, paymaster_spend
    ''',
                                chain=chain)

    stat_deployments = [{
      "NUM_DEPLOYMENTS": summary_stats[0]["NUM_DEPLOYMENTS"]
    }]

    stat_userops = [{"NUM_USEROPS": summary_stats[0]["NUM_USEROPS"]}]

    stat_txns = [{"NUM_TXNS": summary_stats[0]["NUM_TXNS"]}]

    stat_paymaster_spend = [{"GAS_SPENT": summary_stats[0]["GAS_SPENT"]}]

    monthly_active_accounts = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    COUNT(DISTINCT SENDER) as NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1
    ORDER BY 1
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_userops = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1
    ORDER BY 1 
    ''',
                                  chain=chain,
                                  time=timeframe)

    monthly_paymaster_spend = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', DATE), 'YYYY-MM-DD') as DATE,
    SUM(GAS_SPENT) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_DAY_PAYMASTER_SPEND_CHAIN
    WHERE CHAIN = '{chain}'
    AND DATE > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1
    ORDER BY 1 
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_bundler_revenue = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', DATE), 'YYYY-MM-DD') as DATE,
    SUM(REVENUE) AS REVENUE
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_DAY_BUNDLER_REVENUE_CHAIN
    WHERE CHAIN = '{chain}'
    AND DATE > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1
    ORDER BY 1 
    ''',
                                          chain=chain,
                                          time=timeframe)

    retention = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_{time}_RETENTION
    ORDER BY 1, 3
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
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
      # "userops_by_type": userops_by_type,
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
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
    SELECT BUNDLER_NAME FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    AND BUNDLER_REVENUE_USD < 1000000
    AND BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    )
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                time=timeframe)

    multi_userop_chart = execute_sql('''
    SELECT
        TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
        100*COALESCE(SUM(CASE WHEN NUM_USEROPS > 1 THEN 1 ELSE 0 END) / COUNT(*), 0) as pct_multi_userop
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1
    ORDER BY 1
    ''',
                                     time=timeframe)

    accounts_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(DISTINCT SENDER) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 time=timeframe)

    # frontrun_chart = execute_sql('''
    # SELECT 
    # TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    # BUNDLER_NAME,
    # COUNT(DISTINCT tx_hash) as NUM_BUNDLES
    # FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_FAILED_VALIDATION_OPS
    # GROUP BY 1,2
    # ORDER BY 1
    # ''',
    #                              time=timeframe)

    # frontrun_pct_chart = execute_sql('''
    # WITH failed_ops AS (    
    # SELECT 
    # date_trunc('{time}', BLOCK_TIME) as DATE,
    # COUNT(DISTINCT tx_hash) as NUM_BUNDLES_FAILED
    # FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_FAILED_VALIDATION_OPS
    # GROUP BY 1
    # ),
    
    # all_ops AS (
    # SELECT 
    # date_trunc('{time}', BLOCK_TIME) as DATE,
    # COUNT(DISTINCT tx_hash) as NUM_BUNDLES_ALL
    # FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ENTRYPOINT_TRANSACTIONS
    # GROUP BY 1
    # )
    
    # SELECT
    #     TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    # 100 * NUM_BUNDLES_FAILED/NUM_BUNDLES_ALL AS PCT_FRONTRUN
    # FROM all_ops a
    # INNER JOIN failed_ops f 
    # ON a.DATE = f.DATE
    # ORDER BY 1
    # ''',
    #                                  time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "revenue_chart": revenue_chart,
      "multi_userop_chart": multi_userop_chart,
      "accounts_chart": accounts_chart
      # "frontrun_chart": frontrun_chart,
      # "frontrun_pct_chart": frontrun_pct_chart
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
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    AND BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1,2
    ORDER BY 1
    ''',
                                 chain=chain,
                                 time=timeframe)

    # frontrun_chart = execute_sql('''
    # SELECT 
    # TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    # BUNDLER_NAME,
    # COUNT(DISTINCT tx_hash) as NUM_BUNDLES
    # FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_FAILED_VALIDATION_OPS
    # GROUP BY 1,2
    # ORDER BY 1
    # ''',
    #                              chain=chain,
    #                              time=timeframe)

    # frontrun_pct_chart = execute_sql('''
    # WITH failed_ops AS (    
    # SELECT 
    # date_trunc('{time}', BLOCK_TIME) as DATE,
    # COUNT(DISTINCT tx_hash) as NUM_BUNDLES_FAILED
    # FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_FAILED_VALIDATION_OPS
    # GROUP BY 1
    # ),
    
    # all_ops AS (
    # SELECT 
    # date_trunc('{time}', BLOCK_TIME) as DATE,
    # COUNT(DISTINCT tx_hash) as NUM_BUNDLES_ALL
    # FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    # GROUP BY 1
    # )
    
    # SELECT
    # TO_VARCHAR(a.DATE, 'YYYY-MM-DD') AS DATE,
    # 100 * NUM_BUNDLES_FAILED/NUM_BUNDLES_ALL  AS PCT_FRONTRUN
    # FROM all_ops a
    # INNER JOIN failed_ops f 
    # ON a.DATE = f.DATE
    # ORDER BY 1
    # ''',
    #                                  chain=chain,
                                     # time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "revenue_chart": revenue_chart,
      "multi_userop_chart": multi_userop_chart,
      "accounts_chart": accounts_chart,
      # "frontrun_chart": frontrun_chart,
      # "frontrun_pct_chart": frontrun_pct_chart
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    AND BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000000000
    AND BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    )
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                                   time=timeframe)

    # userops_type_chart = execute_sql('''
    # SELECT 
    # TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    # CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
    #      WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
    #      WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
    #      WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
    #      ELSE PAYMASTER_TYPE
    # END AS PAYMASTER_TYPE,
    # COUNT(*) AS NUM_USEROPS
    # FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
    # GROUP BY 1,2
    # ORDER BY 1
    # ''',
    #                                  time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "spend_chart": spend_chart,
      "accounts_chart": accounts_chart,
      "spend_type_chart": spend_type_chart,
      # "userops_type_chart": userops_type_chart
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
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    AND BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    AND BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
    GROUP BY 1,2
    ORDER BY 1 
    ''',
                                   chain=chain,
                                   time=timeframe)

    # userops_type_chart = execute_sql('''
    # SELECT 
    # TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    # CASE WHEN PAYMASTER_TYPE = 'both' THEN 'unlabeled'
    #      WHEN PAYMASTER_TYPE = 'Unknown' THEN 'unlabeled'
    #      WHEN PAYMASTER_TYPE = 'verifying' THEN 'Sponsored'
    #      WHEN PAYMASTER_TYPE = 'token' THEN 'ERC20'
    #      ELSE PAYMASTER_TYPE
    # END AS PAYMASTER_TYPE,
    # COUNT(*) AS NUM_USEROPS
    # FROM  BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    # GROUP BY 1,2
    # ORDER BY 1
    # ''',
    #                                  chain=chain,
    #                                  time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "spend_chart": spend_chart,
      "accounts_chart": accounts_chart,
      "spend_type_chart": spend_type_chart,
      # "userops_type_chart": userops_type_chart,
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
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_DEPLOYMENTS
    GROUP BY 1
    ORDER BY 2 DESC
    ''')

    deployments_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    FACTORY_NAME AS DEPLOYER_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_DEPLOYMENTS
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
        FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
        INNER JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_ACCOUNT_DEPLOYMENTS ad
            ON ad.ACCOUNT_ADDRESS = u.SENDER
            AND ad.CHAIN = u.CHAIN
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_FACTORIES l
            ON l.ADDRESS = ad.FACTORY
        WHERE u.BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
    WHERE u.BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '24 months'
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
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
      WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '6 months'
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

    ops_chart = execute_sql('''
    WITH CombinedUserOps AS (
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
      WHERE BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '6 months'
    ),
    RankedProjects AS (
      SELECT 
        DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(*) AS NUM_OPS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIME) ORDER BY COUNT(*) DESC) AS RN
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
        SUM(NUM_OPS) AS NUM_OPS
      FROM 
        RankedProjects
      GROUP BY 
        1, 2
    )
    SELECT 
      TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, PROJECT, NUM_OPS
    FROM 
      GroupedProjects
    ORDER BY 
      DATE DESC, NUM_OPS DESC;
    ''',
                            time=timeframe)

    ops_paymaster_chart = execute_sql('''
    WITH CombinedUserOps AS (
      SELECT BLOCK_TIME, CALLED_CONTRACT, SENDER FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS
      WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
      AND BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE) - INTERVAL '6 months'
    ),
    RankedProjects AS (
      SELECT 
        DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(*) AS NUM_OPS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIME) ORDER BY COUNT(*) DESC) AS RN
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
        SUM(NUM_OPS) AS NUM_OPS
      FROM 
        RankedProjects
      GROUP BY 
        1, 2
    )
    SELECT 
      TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, PROJECT, NUM_OPS
    FROM 
      GroupedProjects
    ORDER BY 
      DATE DESC, NUM_OPS DESC;
    ''',
                                      time=timeframe)

    leaderboard = execute_sql('''
    SELECT 
    COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
    COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
    COUNT(u.OP_HASH) AS NUM_OPS,
    ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
    FROM 
    BUNDLEBEAR.DBT_KOFI.ERC4337_ALL_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
    ''')

    response_data = {
      "usage_chart": usage_chart,
      "leaderboard": leaderboard,
      "ops_chart": ops_chart,
      "ops_paymaster_chart": ops_paymaster_chart
    }

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
        WHERE u.BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '6 months'
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

    ops_chart = execute_sql('''
    WITH RankedProjects AS (
      SELECT 
        DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(*) AS NUM_OPS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIME) ORDER BY COUNT(*) DESC) AS RN
      FROM 
        BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE u.BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '6 months'
      GROUP BY 
        1, 2
    ),
    GroupedProjects AS (
      SELECT 
        DATE, 
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_OPS) AS NUM_OPS
      FROM 
        RankedProjects
      GROUP BY 
        1, 2
    )
    SELECT 
      TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, PROJECT, NUM_OPS
    FROM 
      GroupedProjects
    ORDER BY 
      DATE DESC, NUM_OPS DESC;
    ''',
                            chain=chain,
                            time=timeframe)

    ops_paymaster_chart = execute_sql('''
    WITH RankedProjects AS (
      SELECT 
        DATE_TRUNC('{time}', u.BLOCK_TIME) AS DATE,
        COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
        COUNT(*) AS NUM_OPS,
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('{time}', u.BLOCK_TIME) ORDER BY COUNT(*) DESC) AS RN
      FROM 
        BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
        LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
        WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
        AND u.BLOCK_TIME > DATE_TRUNC('{time}', CURRENT_DATE()) - INTERVAL '6 months'
      GROUP BY 
        1, 2
    ),
    GroupedProjects AS (
      SELECT 
        DATE, 
        CASE WHEN RN <= 5 THEN PROJECT ELSE 'Other' END AS PROJECT,
        SUM(NUM_OPS) AS NUM_OPS
      FROM 
        RankedProjects
      GROUP BY 
        1, 2
    )
    SELECT 
      TO_VARCHAR(DATE, 'YYYY-MM-DD') as DATE, PROJECT, NUM_OPS
    FROM 
      GroupedProjects
    ORDER BY 
      DATE DESC, NUM_OPS DESC;
    ''',
                                      chain=chain,
                                      time=timeframe)

    leaderboard = execute_sql('''
    SELECT 
    COALESCE(l.NAME, u.CALLED_CONTRACT) AS PROJECT,
    COUNT(DISTINCT u.SENDER) AS NUM_UNIQUE_SENDERS,
    COUNT(u.OP_HASH) AS NUM_OPS,
    ROW_NUMBER() OVER(ORDER BY COUNT(DISTINCT u.SENDER) DESC) AS RN
    FROM 
    BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS u
    LEFT JOIN BUNDLEBEAR.DBT_KOFI.ERC4337_LABELS_APPS l ON u.CALLED_CONTRACT = l.ADDRESS
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
    ''',
                              chain=chain)

    response_data = {
      "usage_chart": usage_chart,
      "leaderboard": leaderboard,
      "ops_chart": ops_chart,
      "ops_paymaster_chart": ops_paymaster_chart
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

@app.route('/eip7702-overview')
@cache.memoize(make_name=make_cache_key)
def eip7702_overview():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  if chain == 'all':
    summary_stats = execute_sql('''
    SELECT 
    LIVE_SMART_WALLETS,
    NUM_AUTHORIZATIONS,
    NUM_AUTHORIZED_CONTRACTS,
    NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_METRICS_TOTAL_SUMMARY
    ''')

    stat_live_smart_wallets = [{
      "LIVE_SMART_WALLETS": summary_stats[0]["LIVE_SMART_WALLETS"]
    }]

    stat_authorizations = [{"NUM_AUTHORIZATIONS": summary_stats[0]["NUM_AUTHORIZATIONS"]}]

    stat_set_code_txns = [{"NUM_SET_CODE_TXNS": summary_stats[0]["NUM_SET_CODE_TXNS"]}]

    activity_query = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_DATE), 'YYYY-MM-DD') AS DATE,
    CHAIN,
    COUNT(*) AS NUM_AUTHORIZATIONS,
    COUNT(DISTINCT TX_HASH) AS NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_ALL_AUTHORIZATIONS
    GROUP BY 1,2
    ORDER BY 1
    ''',time=timeframe)

    authorizations_chart = []
    for row in activity_query:
      authorizations_chart.append({
          "DATE": row["DATE"],
          "CHAIN": row["CHAIN"],
          "NUM_AUTHORIZATIONS": row["NUM_AUTHORIZATIONS"]
      })

    set_code_chart = []
    for row in activity_query:
      set_code_chart.append({
            "DATE": row["DATE"],
            "CHAIN": row["CHAIN"],
            "NUM_SET_CODE_TXNS": row["NUM_SET_CODE_TXNS"]
        })

    state_query = execute_sql('''
    SELECT
    TO_VARCHAR(DAY, 'YYYY-MM-DD') AS DATE,
    CHAIN,
    LIVE_SMART_WALLETS,
    LIVE_AUTHORIZED_CONTRACTS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_METRICS_DAILY_ALL_AUTHORITY_STATE
    ORDER BY 1
    ''')

    live_smart_wallets_chart = []
    for row in state_query:
      live_smart_wallets_chart.append({
          "DATE": row["DATE"],
          "CHAIN": row["CHAIN"],
          "LIVE_SMART_WALLETS": row["LIVE_SMART_WALLETS"]
      })

    live_authorized_contracts_chart = []
    for row in state_query:
      live_authorized_contracts_chart.append({
            "DATE": row["DATE"],
            "CHAIN": row["CHAIN"],
            "LIVE_AUTHORIZED_CONTRACTS": row["LIVE_AUTHORIZED_CONTRACTS"]
        })

    response_data = {
      "stat_live_smart_wallets": stat_live_smart_wallets,
      "stat_authorizations": stat_authorizations,
      "stat_set_code_txns": stat_set_code_txns,
      "authorizations_chart": authorizations_chart,
      "set_code_chart": set_code_chart,
      "live_smart_wallets_chart": live_smart_wallets_chart,
      "live_authorized_contracts_chart": live_authorized_contracts_chart
    }
    
    return jsonify(response_data)
  else:
    summary_stats = execute_sql('''
    SELECT 
    COUNT(DISTINCT CASE 
      WHEN rn = 1 AND AUTHORIZED_CONTRACT != '0x0000000000000000000000000000000000000000' 
      THEN AUTHORITY 
    END) AS LIVE_ACCOUNTS,
    COUNT(*) AS NUM_AUTHORIZATIONS,
    COUNT(DISTINCT AUTHORIZED_CONTRACT) AS NUM_AUTHORIZED_CONTRACTS,
    COUNT(DISTINCT TX_HASH) AS NUM_SET_CODE_TXNS
    FROM (
    SELECT 
      AUTHORITY,
      AUTHORIZED_CONTRACT,
      TX_HASH,
      ROW_NUMBER() OVER (PARTITION BY AUTHORITY ORDER BY NONCE DESC, BLOCK_TIME DESC) as rn
    FROM 
      BUNDLEBEAR.DBT_KOFI.EIP7702_{chain}_AUTHORIZATIONS
    )
    ''',chain=chain)

    stat_live_smart_wallets = [{
      "LIVE_SMART_WALLETS": summary_stats[0]["LIVE_SMART_WALLETS"]
    }]

    stat_authorizations = [{"NUM_AUTHORIZATIONS": summary_stats[0]["NUM_AUTHORIZATIONS"]}]

    stat_set_code_txns = [{"NUM_SET_CODE_TXNS": summary_stats[0]["NUM_SET_CODE_TXNS"]}]

    activity_query = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_DATE), 'YYYY-MM-DD') AS DATE,
    COUNT(*) AS NUM_AUTHORIZATIONS,
    COUNT(DISTINCT TX_HASH) AS NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_{chain}_AUTHORIZATIONS
    GROUP BY 1,2
    ORDER BY 1
    ''',chain=chain,time=timeframe)

    authorizations_chart = []
    for row in activity_query:
        authorizations_chart.append({
            "DATE": row["DATE"],
            "NUM_AUTHORIZATIONS": row["NUM_AUTHORIZATIONS"]
        })

    set_code_chart = []
    for row in activity_query:
      set_code_chart.append({
            "DATE": row["DATE"],
            "NUM_SET_CODE_TXNS": row["NUM_SET_CODE_TXNS"]
        })

    state_query = execute_sql('''
    SELECT
    TO_VARCHAR(DAY, 'YYYY-MM-DD') AS DATE,
    LIVE_SMART_WALLETS,
    LIVE_AUTHORIZED_CONTRACTS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_METRICS_DAILY_ALL_AUTHORITY_STATE
    WHERE CHAIN = '{chain}'
    ORDER BY 1
    ''', chain=chain)

    live_smart_wallets_chart = []
    for row in state_query:
      live_smart_wallets_chart.append({
          "DATE": row["DATE"],
          "LIVE_SMART_WALLETS": row["LIVE_SMART_WALLETS"]
      })

    live_authorized_contracts_chart = []
    for row in state_query:
      live_authorized_contracts_chart.append({
            "DATE": row["DATE"],
            "LIVE_AUTHORIZED_CONTRACTS": row["LIVE_AUTHORIZED_CONTRACTS"]
        })

    response_data = {
      "stat_live_smart_wallets": stat_live_smart_wallets,
      "stat_authorizations": stat_authorizations,
      "stat_set_code_txns": stat_set_code_txns,
      "authorizations_chart": authorizations_chart,
      "set_code_chart": set_code_chart,
      "live_smart_wallets_chart": live_smart_wallets_chart,
      "live_authorized_contracts_chart": live_authorized_contracts_chart
    }

    return jsonify(response_data)

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)

# REQUIREMENTS:
# 1. TO GET SNOWFLAKE
# POETRY ADD snowflake-connector-python
# 2. TO GET SSL
# sed -i '/    ];/i\      pkgs.openssl.out' replit.nix
