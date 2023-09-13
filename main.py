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
                                     schema="ERC4337")

  sql = sql_string.format(**kwargs)
  res = conn.cursor(DictCursor).execute(sql)
  results = res.fetchall()
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
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    
    UNION ALL
    SELECT 
    'ethereum' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    
    UNION ALL
    SELECT 
    'optimism' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    
    UNION ALL
    SELECT 
    'polygon' as chain,
    BLOCK_TIME,
    TX_HASH
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
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
    GROUP BY 1,2)
    ORDER BY 1
    ''', chain=chain, time=timeframe)
  
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
    )
    ORDER BY 1
    ''', chain=chain, time=timeframe)
  
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
    )
    ORDER BY 1
    ''', chain=chain, time=timeframe)
  
    response_data = {
      "deployments": stat_deployments,
      "userops": stat_userops,
      "transactions": stat_txns,
      "monthly_active_accounts": monthly_active_accounts,
      "monthly_userops": monthly_userops,
      "monthly_paymaster_spend": monthly_paymaster_spend
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
  
    monthly_active_accounts = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    COUNT(DISTINCT SENDER) as NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1
    ORDER BY 1
    ''', chain=chain, time=timeframe)
  
    monthly_userops = execute_sql('''
    SELECT
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    COUNT(*) as NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1
    ORDER BY 1
    ''', chain=chain, time=timeframe)
  
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
    ''', chain=chain, time=timeframe)
  
    response_data = {
      "deployments": stat_deployments,
      "userops": stat_userops,
      "transactions": stat_txns,
      "monthly_active_accounts": monthly_active_accounts,
      "monthly_userops": monthly_userops,
      "monthly_paymaster_spend": monthly_paymaster_spend
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
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
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
    )
    GROUP BY 1,2
    ORDER BY 1
    ''', time=timeframe)

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
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    UNION ALL 
    SELECT BUNDLER_NAME, BUNDLER_REVENUE_USD, BLOCK_TIME 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    )
    GROUP BY 1,2
    ORDER BY 1
    ''', time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "revenue_chart": revenue_chart
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
    ''', chain=chain)

    userops_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    ORDER BY 1
    ''', chain=chain, time=timeframe)

    revenue_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    BUNDLER_NAME,
    SUM(BUNDLER_REVENUE_USD) AS REVENUE
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ENTRYPOINT_TRANSACTIONS
    WHERE BUNDLER_REVENUE_USD != 'NaN'
    GROUP BY 1,2
    ORDER BY 1
    ''', chain=chain, time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "revenue_chart": revenue_chart
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
    AND ACTUALGASCOST_USD < 1000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    UNION ALL 
    SELECT PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    WHERE PAYMASTER != '0x0000000000000000000000000000000000000000'
    AND ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
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
    )
    GROUP BY 1,2
    ORDER BY 1
    ''', time=timeframe)

    spend_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM
    (
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD 
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_POLYGON_USEROPS
    WHERE ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OPTIMISM_USEROPS
    WHERE ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ARBITRUM_USEROPS
    WHERE ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    UNION ALL 
    SELECT BLOCK_TIME, PAYMASTER_NAME, ACTUALGASCOST_USD  
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_ETHEREUM_USEROPS
    WHERE ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    )
    GROUP BY 1,2
    ORDER BY 1 
    ''', time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "spend_chart": spend_chart
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
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1
    ORDER BY 3 DESC
    ''', chain=chain)

    userops_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    COUNT(*) AS NUM_USEROPS
    FROM  BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    GROUP BY 1,2
    ORDER BY 1
    ''', chain=chain, time=timeframe)

    spend_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    PAYMASTER_NAME,
    SUM(ACTUALGASCOST_USD) AS GAS_SPENT
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_USEROPS
    WHERE ACTUALGASCOST_USD != 'NaN'
    AND ACTUALGASCOST_USD < 1000
    GROUP BY 1,2
    ORDER BY 1 
    ''', chain=chain, time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "userops_chart": userops_chart,
      "spend_chart": spend_chart
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
    FACTORY_NAME,
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
    )
    GROUP BY 1
    ORDER BY 2 DESC
    ''')

    deployments_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    FACTORY_NAME,
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
    )
    GROUP BY 1,2
    ORDER BY 1
    ''', time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "deployments_chart": deployments_chart
    }
  
    return jsonify(response_data)
    
  else:
    leaderboard = execute_sql('''
    SELECT 
    FACTORY_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS
    GROUP BY 1
    ORDER BY 2 DESC
    ''', chain=chain)

    deployments_chart = execute_sql('''
    SELECT 
    TO_VARCHAR(date_trunc('{time}', BLOCK_TIME), 'YYYY-MM-DD') as DATE,
    FACTORY_NAME,
    COUNT(*) AS NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.ERC4337_{chain}_ACCOUNT_DEPLOYMENTS
    GROUP BY 1,2
    ORDER BY 1
    ''', chain=chain, time=timeframe)

    response_data = {
      "leaderboard": leaderboard,
      "deployments_chart": deployments_chart
    }
  
    return jsonify(response_data)

app.run(host='0.0.0.0', port=81)

# REQUIREMENTS:
# 1. TO GET SNOWFLAKE
# POETRY ADD snowflake-connector-python
# 2. TO GET SSL
# sed -i '/    ];/i\      pkgs.openssl.out' replit.nix
