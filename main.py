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
  "CACHE_DEFAULT_TIMEOUT": 57600,
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
                                     schema="DBT_KOFI",
                                     disable_ocsp_checks=True)

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

  summary_stats = execute_sql('''
  SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_SUMMARY_STATS_METRIC
  WHERE CHAIN = '{chain}'
  ''', chain=chain)

  stat_accounts = [{ "NUM_ACCOUNTS": summary_stats[0]["NUM_ACCOUNTS"] }]

  stat_userops = [{"NUM_USEROPS": summary_stats[0]["NUM_USEROPS"]}]

  stat_txns = [{"NUM_TXNS": summary_stats[0]["NUM_TXNS"]}]

  stat_paymaster_spend = [{"GAS_SPENT": summary_stats[0]["GAS_SPENT"]}]

  accounts_by_category = execute_sql('''
  SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_ACCOUNTS_BY_CATEGORY_METRIC
  WHERE TIMEFRAME = '{time}'
  AND CHAIN = '{chain}'                                      
  ORDER BY DATE
  ''',
                                        chain=chain,
                                        time=timeframe)                        

  if chain == 'all':
    monthly_active_accounts = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_ACTIVE_ACCOUNTS_METRIC
    WHERE TIMEFRAME = '{time}'                                     
    ORDER BY DATE
    ''',
                                          time=timeframe)

    monthly_userops = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_USEROPS_METRIC
    WHERE TIMEFRAME = '{time}'                                     
    ORDER BY DATE
    ''',
                                          time=timeframe)

    monthly_paymaster_spend = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_PAYMASTER_SPEND_METRIC
    WHERE TIMEFRAME = '{time}'                                     
    ORDER BY DATE
    ''',
                                          time=timeframe)

    monthly_bundler_revenue = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_BUNDLER_REVENUE_METRIC
    WHERE TIMEFRAME = '{time}'                                     
    ORDER BY DATE
    ''',
                                          time=timeframe)

  else:
    monthly_active_accounts = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_ACTIVE_ACCOUNTS_METRIC
    WHERE TIMEFRAME = '{time}'
    AND CHAIN = '{chain}'                                                                          
    ORDER BY DATE
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_userops = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_USEROPS_METRIC
    WHERE TIMEFRAME = '{time}'     
    AND CHAIN = '{chain}'                                                                 
    ORDER BY DATE
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_paymaster_spend = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_PAYMASTER_SPEND_METRIC
    WHERE TIMEFRAME = '{time}'
    AND CHAIN = '{chain}'                                        
    ORDER BY DATE
    ''',
                                          chain=chain,
                                          time=timeframe)

    monthly_bundler_revenue = execute_sql('''
    SELECT * FROM BUNDLEBEAR.DBT_KOFI.ERC4337_OVERVIEW_BUNDLER_REVENUE_METRIC
    WHERE TIMEFRAME = '{time}' 
    AND CHAIN = '{chain}'                                       
    ORDER BY DATE
    ''',
                                           chain=chain,
                                          time=timeframe)

  response_data = {
    "accounts": stat_accounts,
    "userops": stat_userops,
    "transactions": stat_txns,
    "paymaster_spend": stat_paymaster_spend,
    "monthly_active_accounts": monthly_active_accounts,
    "monthly_userops": monthly_userops,
    "monthly_paymaster_spend": monthly_paymaster_spend,
    "monthly_bundler_revenue": monthly_bundler_revenue,
    "accounts_by_category": accounts_by_category
  }

  return jsonify(response_data)


@app.route('/bundler')
@cache.memoize(make_name=make_cache_key)
def bundler():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  leaderboard = execute_sql('''
  SELECT 
  BUNDLER_NAME,
  NUM_USEROPS,
  NUM_TXNS,
  REVENUE
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BUNDLER_LEADERBOARD_METRIC
  WHERE CHAIN = '{chain}'
  ORDER BY 2 DESC
  ''', chain=chain)

  userops_chart = execute_sql('''
  SELECT
  DATE,
  BUNDLER_NAME,
  NUM_USEROPS
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BUNDLER_USEROPS_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  revenue_chart = execute_sql('''
  SELECT
  DATE,
  BUNDLER_NAME,
  REVENUE
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BUNDLER_REVENUE_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  multi_userop_chart = execute_sql('''
  SELECT
  DATE,
  PCT_MULTI_USEROP
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BUNDLER_MULTIOP_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  accounts_chart = execute_sql('''
  SELECT
  DATE,
  BUNDLER_NAME,
  NUM_ACCOUNTS
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BUNDLER_ACCOUNTS_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  frontrun_chart = execute_sql('''
  SELECT
  DATE,
  BUNDLER_NAME,
  NUM_BUNDLES
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BUNDLER_FRONTRUN_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  frontrun_pct_chart = execute_sql('''
  SELECT
  DATE,
  PCT_FRONTRUN
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_BUNDLER_FRONTRUN_PCT_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
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

  leaderboard = execute_sql('''
  SELECT 
  PAYMASTER_NAME,
  NUM_USEROPS,
  GAS_SPENT
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_PAYMASTER_LEADERBOARD_METRIC
  WHERE CHAIN = '{chain}'
  ORDER BY 3 DESC
  ''', chain=chain)

  userops_chart = execute_sql('''
  SELECT
  DATE,
  PAYMASTER_NAME,
  NUM_USEROPS
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_PAYMASTER_USEROPS_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  spend_chart = execute_sql('''
  SELECT
  DATE,
  PAYMASTER_NAME,
  GAS_SPENT
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_PAYMASTER_SPEND_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  accounts_chart = execute_sql('''
  SELECT
  DATE,
  PAYMASTER_NAME,
  NUM_ACCOUNTS
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_PAYMASTER_ACCOUNTS_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  spend_type_chart = execute_sql('''
  SELECT
  DATE,
  PAYMASTER_TYPE,
  GAS_SPENT
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_PAYMASTER_SPEND_TYPE_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1
  ''',
                                           chain=chain,
                                          time=timeframe)

  response_data = {
    "leaderboard": leaderboard,
    "userops_chart": userops_chart,
    "spend_chart": spend_chart,
    "accounts_chart": accounts_chart,
    "spend_type_chart": spend_type_chart
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

  usage_chart = execute_sql('''
  SELECT
  DATE,
  PROJECT,
  NUM_UNIQUE_SENDERS
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_APPS_USAGE_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1,3
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  ops_chart = execute_sql('''
  SELECT
  DATE,
  PROJECT,
  NUM_OPS
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_APPS_OPS_METRIC
  WHERE CHAIN = '{chain}'
  AND TIMEFRAME = '{time}'
  ORDER BY 1,3
  ''',
                                           chain=chain,
                                          time=timeframe)
  
  leaderboard = execute_sql('''
  SELECT
  PROJECT,
  NUM_UNIQUE_SENDERS,
  NUM_OPS
  FROM BUNDLEBEAR.DBT_KOFI.ERC4337_APPS_LEADERBOARD_METRIC
  WHERE CHAIN = '{chain}'
  ORDER BY 2 DESC
  ''',
                                           chain=chain)

  response_data = {
    "usage_chart": usage_chart,
    "leaderboard": leaderboard,
    "ops_chart": ops_chart
  }

  return jsonify(response_data)


@app.route('/eip7702-overview')
@cache.memoize(make_name=make_cache_key)
def eip7702_overview():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  summary_stats = execute_sql('''
  SELECT 
  LIVE_SMART_WALLETS,
  NUM_AUTHORIZATIONS,
  NUM_SET_CODE_TXNS
  FROM BUNDLEBEAR.DBT_KOFI.EIP7702_METRICS_TOTAL_SUMMARY
  WHERE CHAIN = '{chain}'
  ''',chain=chain)

  stat_live_smart_wallets = [{ "LIVE_SMART_WALLETS": summary_stats[0]["LIVE_SMART_WALLETS"] }]

  stat_authorizations = [{"NUM_AUTHORIZATIONS": summary_stats[0]["NUM_AUTHORIZATIONS"]}]

  stat_set_code_txns = [{"NUM_SET_CODE_TXNS": summary_stats[0]["NUM_SET_CODE_TXNS"]}]

  smart_wallet_actions_type = execute_sql('''
  SELECT
  DATE,
  TYPE,
  NUM_ACTIONS
  FROM BUNDLEBEAR.DBT_KOFI.EIP7702_OVERVIEW_ACTIONS_TYPE_METRIC
  WHERE TIMEFRAME = '{time}'
  AND CHAIN = '{chain}'
  ORDER BY 1 
  ''', time=timeframe,chain=chain)

  if chain == 'all':
    activity_query = execute_sql('''
    SELECT 
    DATE,
    CHAIN,
    NUM_AUTHORIZATIONS,
    NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_OVERVIEW_ACTIVITY_METRIC
    WHERE TIMEFRAME = '{time}'
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
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_METRICS_DAILY_AUTHORITY_STATE
    WHERE CHAIN != 'cross-chain'
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

    active_smart_wallets_chart = execute_sql('''
    SELECT
    DATE,
    CHAIN,
    ACTIVE_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_OVERVIEW_ACTIVE_WALLETS_METRIC
    WHERE TIMEFRAME = '{time}'
    ORDER BY 1 
    ''', time=timeframe)

    smart_wallet_actions = execute_sql('''
    SELECT
    DATE,
    CHAIN,
    NUM_ACTIONS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_OVERVIEW_ACTIONS_METRIC
    WHERE TIMEFRAME = '{time}'
    ORDER BY 1 
    ''', time=timeframe)

  else:
    activity_query = execute_sql('''                               
    SELECT 
    DATE,
    NUM_AUTHORIZATIONS,
    NUM_SET_CODE_TXNS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_OVERVIEW_ACTIVITY_METRIC
    WHERE CHAIN = '{chain}'
    AND TIMEFRAME = '{time}'
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
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_METRICS_DAILY_AUTHORITY_STATE
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

    active_smart_wallets_chart = execute_sql('''
    SELECT
    DATE,
    ACTIVE_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_OVERVIEW_ACTIVE_WALLETS_METRIC
    WHERE TIMEFRAME = '{time}'
    AND CHAIN = '{chain}'
    ORDER BY 1 
    ''', time=timeframe, chain=chain)

    smart_wallet_actions = execute_sql('''
    SELECT
    DATE,
    NUM_ACTIONS
    FROM BUNDLEBEAR.DBT_KOFI.EIP7702_OVERVIEW_ACTIONS_METRIC
    WHERE TIMEFRAME = '{time}'
    AND CHAIN = '{chain}'
    ORDER BY 1 
    ''', time=timeframe, chain=chain)

  response_data = {
    "stat_live_smart_wallets": stat_live_smart_wallets,
    "stat_authorizations": stat_authorizations,
    "stat_set_code_txns": stat_set_code_txns,
    "authorizations_chart": authorizations_chart,
    "set_code_chart": set_code_chart,
    "live_smart_wallets_chart": live_smart_wallets_chart,
    "live_authorized_contracts_chart": live_authorized_contracts_chart,
    "active_smart_wallets_chart": active_smart_wallets_chart,
    "smart_wallet_actions": smart_wallet_actions,
    "smart_wallet_actions_type": smart_wallet_actions_type
  }

  return jsonify(response_data)

@app.route('/eip7702-authorized-contracts')
@cache.memoize(make_name=make_cache_key)
def eip7702_authorized_contracts():
  chain = request.args.get('chain', 'all')

  leaderboard = execute_sql('''
  SELECT
  AUTHORIZED_CONTRACT,
  NUM_WALLETS
  FROM BUNDLEBEAR.DBT_KOFI.EIP7702_AUTH_CONTRACT_LEADERBOARD_METRIC
  WHERE CHAIN = '{chain}'
  ORDER BY 2 DESC
  ''',chain=chain)

  live_smart_wallets_chart = execute_sql('''
  SELECT
  DATE,
  AUTHORIZED_CONTRACT,
  NUM_WALLETS
  FROM BUNDLEBEAR.DBT_KOFI.EIP7702_AUTH_CONTRACT_LIVE_WALLETS_METRIC
  WHERE CHAIN = '{chain}'
  ORDER BY 1     
  ''',chain=chain)


  response_data = {
    "leaderboard": leaderboard,
    "live_smart_wallets_chart": live_smart_wallets_chart
  }

  return jsonify(response_data)

@app.route('/eip7702-apps')
@cache.memoize(make_name=make_cache_key)
def eip7702_apps():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  usage_chart = execute_sql('''
  SELECT
  DATE,
  PROJECT,
  NUM_UNIQUE_SENDERS
  FROM BUNDLEBEAR.DBT_KOFI.EIP7702_APPS_USAGE_METRIC         
  WHERE TIMEFRAME = '{time}'  
  AND CHAIN = '{chain}'  
  ORDER BY 1                                                                                        
  ''', time=timeframe, chain=chain)

  noncrime_usage_chart = execute_sql('''
  SELECT
  DATE,
  PROJECT,
  NUM_UNIQUE_SENDERS
  FROM BUNDLEBEAR.DBT_KOFI.EIP7702_APPS_NONCRIME_USAGE_METRIC         
  WHERE TIMEFRAME = '{time}'  
  AND CHAIN = '{chain}'  
  ORDER BY 1                                                                                     
  ''', time=timeframe, chain=chain)

  response_data = {
    "usage_chart": usage_chart,
    "noncrime_usage_chart": noncrime_usage_chart
  }
  
  return jsonify(response_data)
    
@app.route('/erc4337-activation')
@cache.memoize(make_name=make_cache_key)
def erc4337_activation():
  chain = request.args.get('chain', 'all')
  timeframe = request.args.get('timeframe', 'week')

  new_users_provider_chart = execute_sql('''
  SELECT
  DATE,
  PROVIDER,
  NUM_ACCOUNTS
  FROM BUNDLEBEAR.DBT_KOFI.erc4337_activation_new_accounts_metric         
  WHERE TIMEFRAME = '{time}'  
  AND CHAIN = '{chain}'  
  ORDER BY 1                                                                                     
  ''', time=timeframe, chain=chain)

  if chain == 'all':
    new_users_chain_chart = execute_sql('''
    SELECT
    DATE,
    CHAIN,
    NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.erc4337_activation_new_accounts_chain_metric         
    WHERE TIMEFRAME = '{time}' 
    ORDER BY 1                                                                                      
    ''', time=timeframe)
  else:
    new_users_chain_chart = execute_sql('''
    SELECT
    DATE,
    NUM_ACCOUNTS
    FROM BUNDLEBEAR.DBT_KOFI.erc4337_activation_new_accounts_chain_metric         
    WHERE TIMEFRAME = '{time}'                                                                                           
    AND CHAIN = '{chain}'     
    ORDER BY 1                                                                                    
    ''', time=timeframe, chain=chain)

  response_data = {
    "new_users_provider_chart": new_users_provider_chart,
    "new_users_chain_chart": new_users_chain_chart
  }
  
  return jsonify(response_data)
    
if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)

# REQUIREMENTS:
# 1. TO GET SNOWFLAKE
# POETRY ADD snowflake-connector-python
# 2. TO GET SSL
# sed -i '/    ];/i\      pkgs.openssl.out' replit.nix
