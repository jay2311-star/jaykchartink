import pandas as pd
import os
import sqlite3
import json
import requests
from dhanhq import dhanhq
import yfinance as yf
from datetime import datetime, timedelta, time


import traceback
import redis
import logging
from redis import Redis, ConnectionError, RedisError
import pytz
from sqlalchemy import create_engine, text


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_BASE_URL = "http://139.59.70.202:5000"  # Replace with your droplet's IP if different

# Your login credentials
username = "c.jaykrishnan@gmail.com"
password = "Pest@123"

# Database connection details
host = 'mydb.cb04giyquztt.ap-south-1.rds.amazonaws.com'
user = 'admin'
password = 'Pest1234'
database = 'mydb'

# Constants
NSE = "NSE"
NSE_FNO = "NSE_FNO"
NSE_EQ = "NSE_EQ"
BUY = "BUY"
SELL = "SELL"
MARKET = "MARKET"
LIMIT = "LIMIT"
STOP_LOSS_MARKET = "STOP_LOSS_MARKET"
MARGIN = "MARGIN"
TICK_SIZE = 0.05  # Tick size for most Indian stocks

# Access Dhan API client credentials from environment variables
CLIENT_ID = os.getenv('DHAN_API_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_API_ACCESS_TOKEN')

# Redis connection details
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_RETRY_ATTEMPTS = 3
REDIS_RETRY_DELAY = 1

# Create a Redis client
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client.ping()
    logging.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.ConnectionError as e:
    logging.error(f"Failed to connect to Redis: {e}")
    redis_client = None

def get_db_connection():
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        return engine
    except Exception as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        return None




def get_strategy_config(strategy_name):
    engine = get_db_connection()
    if engine:
        try:
            query = "SELECT * FROM Controls WHERE Strategy = %s"
            strategy_config = pd.read_sql(query, engine, params=(strategy_name,))
            if not strategy_config.empty:
                config = strategy_config.iloc[0].to_dict()
                # Convert Timedelta objects to string
                if isinstance(config['Start'], pd.Timedelta):
                    config['Start'] = str(config['Start']).split()[-1]
                if isinstance(config['Stop'], pd.Timedelta):
                    config['Stop'] = str(config['Stop']).split()[-1]
                # Ensure Holding_Period, Cycle_time_in_mins, Max_Stock_Position_Size, and Maxinastrategy are included
                if 'Holding_Period' not in config:
                    config['Holding_Period'] = 'day'  # Default to 'day' if not specified
                if 'Cycle_time_in_mins' not in config:
                    config['Cycle_time_in_mins'] = None  # Default to None if not specified
                if 'Max_Stock_Position_Size' not in config:
                    config['Max_Stock_Position_Size'] = None  # Default to None if not specified
                if 'Maxinastrategy' not in config:
                    config['Maxinastrategy'] = None  # Default to None if not specified
                return config
            return None
        finally:
            engine.dispose()
    return None

def get_trading_list():
    engine = get_db_connection()
    if engine is not None:
        try:
            query = "SELECT * FROM api_scrip_master"
            trading_list_df = pd.read_sql(query, engine)
            return trading_list_df
        except Exception as e:
            logging.error(f"Error while fetching trading list: {e}")
            return None
        finally:
            if engine is not None:
                engine.dispose()
    else:
        return None

def get_lots():
    engine = get_db_connection()
    if engine is not None:
        try:
            query = "SELECT * FROM Lots"
            lots_df = pd.read_sql(query, engine)
            return lots_df
        except Exception as e:
            logging.error(f"Error while fetching lots: {e}")
            return None
        finally:
            if engine is not None:
                engine.dispose()
    else:
        return None

def get_sector_and_industry(symbol):
    engine = get_db_connection()
    if engine is not None:
        try:
            symbol_ns = symbol if symbol.endswith('.NS') else f"{symbol}.NS"
            query = "SELECT Sector, Industry FROM sector_industry WHERE Symbol = %s"
            df = pd.read_sql(query, engine, params=(symbol_ns,))
            if not df.empty:
                return df.iloc[0]['Sector'], df.iloc[0]['Industry']
            else:
                return "Unknown", "Unknown"
        except Exception as e:
            logging.error(f"Error while fetching sector and industry for {symbol_ns}: {e}")
            return "Unknown", "Unknown"
        finally:
            if engine is not None:
                engine.dispose()
    else:
        return "Unknown", "Unknown"

def check_existing_trades(symbol, strategy, engine):
    try:
        with engine.connect() as connection:
            query = text("""
            SELECT COUNT(*) as trade_count
            FROM trades
            WHERE symbol = :symbol
            AND strategy = :strategy
            AND DATE(timestamp + INTERVAL 5 HOUR + INTERVAL 30 MINUTE) = CURDATE()
            """)
            result = connection.execute(query, {"symbol": symbol, "strategy": strategy}).fetchone()
            trade_count = result[0] if result else 0
            
            # Log using UTC time for consistency with the rest of your application
            utc_now = datetime.utcnow()
            logging.info(f"[{utc_now}] Existing trades for {symbol} in strategy {strategy} today (IST): {trade_count}")
            
            return trade_count
    except Exception as e:
        logging.error(f"Error checking existing trades for {symbol} in strategy {strategy}: {e}")
        logging.error(f"Error details: {traceback.format_exc()}")
        return 0

def save_trade_log_to_mysql(trade_entries):
    if not trade_entries:
        logging.warning("No trade entries to save")
        return
    engine = get_db_connection()
    if engine is not None:
        try:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            for entry in trade_entries:
                try:
                    entry['response'] = json.dumps(entry['response'])
                    entry['security_id'] = int(entry['security_id'])
                    entry['quantity'] = int(entry['quantity'])

                    check_query = """
                    SELECT COUNT(*)
                    FROM trades
                    WHERE security_id = %s AND timestamp = %s
                    """
                    cursor.execute(check_query, (entry['security_id'], entry['timestamp']))
                    if cursor.fetchone()[0] > 0:
                        logging.warning(f"Duplicate entry found for security_id {entry['security_id']} at {entry['timestamp']}. Skipping insertion.")
                        continue

                    insert_query = """
                    INSERT INTO trades (
                        timestamp, symbol, strategy, action, security_id, quantity, price, order_type, trigger_price,
                        entry_price, exit_price, stop_loss, target, order_status, response, max_profit,
                        max_loss, trade_type, stop_loss_percentage, target_percentage, atr_sl_multiplier,
                        atr_target_multiplier, product_type, position_size, holding_period, exit_time, realized_profit,
                        planned_exit_datetime, exit_reason
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    cursor.execute(insert_query, (
                        entry['timestamp'], entry['symbol'], entry['strategy'], entry['action'], entry['security_id'],
                        entry['quantity'], entry['price'], entry['order_type'], entry['trigger_price'], entry['entry_price'],
                        entry['exit_price'], entry['stop_loss'], entry['target'], entry['order_status'], entry['response'],
                        entry['max_profit'], entry['max_loss'], entry['trade_type'], entry['stop_loss_percentage'],
                        entry['target_percentage'], entry['atr_sl_multiplier'], entry['atr_target_multiplier'],
                        entry['product_type'], entry['position_size'], entry['holding_period'], entry['exit_time'],
                        entry['realized_profit'], entry['planned_exit_datetime'], entry['exit_reason']
                    ))
                    logging.info(f"Trade entry inserted for {entry['symbol']}")
                except Exception as e:
                    logging.error(f"Error inserting entry into MySQL: {e}")
                    logging.error(f"Problematic entry: {entry}")
            
            connection.commit()
            logging.info("Trade log appended to MySQL database successfully.")
        except Exception as e:
            logging.error(f"Error while saving trade log to database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
            engine.dispose()
    else:
        logging.error("Failed to connect to the database. Trade log not saved.")



def get_price(security_id):
    try:
        response = requests.get(f"{API_BASE_URL}/price/{security_id}")
        if response.status_code == 200:
            price_data = response.json()
            latest_price = price_data.get('latest_price')
            if latest_price:
                return float(latest_price)
        else:
            print(f"Failed to get price for security_id {security_id}. Status code: {response.status_code}")
        return None
    except requests.RequestException as e:
        print(f"Error fetching price: {e}")
        return None

def within_trading_hours(start_time, end_time):
    current_time = datetime.now().time()
    start_time = datetime.strptime(start_time, "%H:%M:%S").time()
    end_time = datetime.strptime(end_time, "%H:%M:%S").time()
    return start_time <= current_time <= end_time

def calculate_atr(data, period=14):
    data['H-L'] = abs(data['High'] - data['Low'])
    data['H-PC'] = abs(data['High'] - data['Close'].shift(1))
    data['L-PC'] = abs(data['Low'] - data['Close'].shift(1))
    data['TR'] = data[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    data['ATR'] = data['TR'].rolling(window=period).mean()
    return data['ATR'].iloc[-1]

def check_sector_industry(sector, industry, strategy_config):
    if strategy_config['sector_in'] is None and strategy_config['industry_in'] is None:
        return True  # If no sector or industry restrictions, allow all
    allowed_sectors = [x.strip() for x in strategy_config['sector_in'].split(',')] if strategy_config['sector_in'] else []
    allowed_industries = [x.strip() for x in strategy_config['industry_in'].split(',')] if strategy_config['industry_in'] else []
    sector_match = not allowed_sectors or sector in allowed_sectors
    industry_match = not allowed_industries or industry in allowed_industries
    return sector_match or industry_match

def place_order(dhan, symbol, security_id, lot_size, entry_price, stop_loss, target, trade_type, strategy_key, product_type, exchange_segment, holding_period, cycle_time_in_mins):
    try:
        logging.info("=" * 50)
        logging.info(f"Attempting to place order for {symbol}")
        logging.info(f"Order details:")
        logging.info(f"  Security ID: {security_id}")
        logging.info(f"  Symbol: {symbol}")
        logging.info(f"  Quantity: {lot_size}")
        logging.info(f"  Entry Price: {entry_price}")
        logging.info(f"  Stop Loss: {stop_loss}")
        logging.info(f"  Target: {target}")
        logging.info(f"  Trade Type: {trade_type}")
        logging.info(f"  Product Type: {product_type}")
        logging.info(f"  Strategy Key: {strategy_key}")
        logging.info(f"  Exchange Segment: {exchange_segment}")
        logging.info(f"  Holding Period: {holding_period}")
        logging.info(f"  Cycle Time in Minutes: {cycle_time_in_mins}")

        transaction_type = BUY if trade_type == 'Long' else SELL

        logging.info("Dhan API call parameters:")
        logging.info(f"  security_id: {str(security_id)}")
        logging.info(f"  exchange_segment: {exchange_segment}")
        logging.info(f"  transaction_type: {transaction_type}")
        logging.info(f"  quantity: {lot_size}")
        logging.info(f"  order_type: {MARKET}")
        logging.info(f"  product_type: {product_type}")
        logging.info(f"  price: 0")
        logging.info(f"  tag: {strategy_key}")

        logging.info("Calling Dhan place_order API...")
        response = dhan.place_order(
            security_id=str(security_id), 
            exchange_segment=exchange_segment,
            transaction_type=transaction_type,
            quantity=lot_size,
            order_type=MARKET,
            product_type=product_type,
            price=0,
            tag=strategy_key
        )
        logging.info(f"Dhan API Response:")
        logging.info(json.dumps(response, indent=2))

        if response and response['status'] == 'success':
            logging.info("Order placement successful")
            
            entry_datetime = datetime.now()
            logging.info(f"Trade entry time: {entry_datetime}")
            
            # Calculate planned exit datetime
            if holding_period.lower() == 'minute':
                if cycle_time_in_mins is not None:
                    try:
                        minutes_to_add = int(cycle_time_in_mins) * 5
                        planned_exit = entry_datetime + timedelta(minutes=minutes_to_add + 30, hours=5)
                        logging.info(f"Planned exit time calculated based on Cycle_time_in_mins: {cycle_time_in_mins}")
                    except ValueError:
                        logging.warning(f"Invalid Cycle_time_in_mins value: {cycle_time_in_mins}. Using default 5 minutes.")
                        planned_exit = entry_datetime + timedelta(minutes=35, hours=5)
                else:
                    logging.info("Cycle_time_in_mins is None. Using default 5 minutes.")
                    planned_exit = entry_datetime + timedelta(minutes=35, hours=5)
            elif holding_period.lower() == 'day':
                planned_exit = (entry_datetime + timedelta(days=1, hours=5, minutes=30)).replace(hour=15, minute=15, second=0, microsecond=0)
            elif holding_period.lower() == 'week':
                planned_exit = (entry_datetime + timedelta(days=5, hours=5, minutes=30)).replace(hour=15, minute=15, second=0, microsecond=0)
            elif holding_period.lower() == 'month':
                planned_exit = (entry_datetime + timedelta(days=20, hours=5, minutes=30)).replace(hour=15, minute=15, second=0, microsecond=0)
            else:
                logging.warning(f"Unknown holding period: {holding_period}. Using default (1 day).")
                planned_exit = (entry_datetime + timedelta(days=1, hours=5, minutes=30)).replace(hour=15, minute=15, second=0, microsecond=0)

            # Ensure planned exit is not later than 3:15 PM IST
            max_exit_time = planned_exit.replace(hour=15, minute=15, second=0, microsecond=0)
            if planned_exit > max_exit_time:
                planned_exit = max_exit_time

            # Ensure planned exit is not in the past
            current_time = datetime.now() + timedelta(hours=5, minutes=30)  # Current time in IST
            if planned_exit <= current_time:
                planned_exit = (current_time.date() + timedelta(days=1)).replace(hour=15, minute=15, second=0, microsecond=0)

            logging.info(f"Calculated planned exit datetime: {planned_exit}")




            trade_entry = {
                'timestamp': entry_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': symbol,
                'strategy': strategy_key,
                'action': trade_type,
                'security_id': security_id,
                'quantity': lot_size,
                'price': entry_price,
                'order_type': MARKET,
                'trigger_price': None,
                'entry_price': entry_price,
                'exit_price': None,
                'stop_loss': stop_loss,
                'target': target,
                'order_status': 'open',
                'response': response,
                'max_profit': abs(target - entry_price) * lot_size,
                'max_loss': abs(stop_loss - entry_price) * lot_size,
                'trade_type': trade_type,
                'stop_loss_percentage': abs((entry_price - stop_loss) / entry_price) * 100,
                'target_percentage': abs((target - entry_price) / entry_price) * 100,
                'atr_sl_multiplier': None,
                'atr_target_multiplier': None,
                'product_type': product_type,
                'position_size': entry_price * lot_size,
                'holding_period': holding_period,
                'exit_time': None,
                'realized_profit': None,
                'planned_exit_datetime': planned_exit,
                'exit_reason': None
            }
            
            logging.info("Saving trade entry to database...")
            save_trade_log_to_mysql([trade_entry])
            logging.info("Trade entry saved successfully")
        else:
            logging.error("Order placement failed")
        
        logging.info("=" * 50)
        return response
    except Exception as e:
        logging.error(f"An error occurred while placing order: {str(e)}")
        logging.error(f"Error details: {traceback.format_exc()}")
        return None

def get_positions(dhan):
    try:
        logging.info("Retrieving current positions")
        response = dhan.get_positions()
        if response['status'] == 'success':
            logging.info("Positions retrieved successfully")
            return response['data']
        else:
            logging.error(f"Failed to retrieve positions: {response['remarks']}")
            return []
    except Exception as e:
        logging.error(f"An error occurred while retrieving portfolio positions: {e}")
        return []

def is_position_open(symbol, strategy, engine):
    try:
        with engine.connect() as connection:
            query = text("""
            SELECT COUNT(*) as open_positions
            FROM trades
            WHERE symbol = :symbol
            AND strategy = :strategy
            AND order_status = 'open'
            """)
            result = connection.execute(query, {"symbol": symbol, "strategy": strategy}).fetchone()
            logging.info(f"Query result: {result}")
            return result[0] > 0
    except Exception as e:
        logging.error(f"Error checking for open position: {e}")
        logging.error(f"Error details: {traceback.format_exc()}")
        return False

def insert_place_order_log(engine, log_data):
    try:
        with engine.connect() as connection:
            columns = ', '.join(log_data.keys())
            placeholders = ':' + ', :'.join(log_data.keys())
            query = text(f"INSERT INTO place_order ({columns}) VALUES ({placeholders})")
            connection.execute(query, log_data)
            connection.commit()
        logging.info(f"Inserted place_order log for {log_data['symbol']}")
    except Exception as e:
        logging.error(f"Error inserting place_order log: {e}")
        logging.error(f"Log data: {log_data}")

def process_trade(dhan, symbol, strategy_config):
    engine = get_db_connection()
    if engine is None:
        logging.error("Failed to establish database connection")
        return

    log_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'symbol': symbol,
        'strategy': strategy_config['Strategy'],
        'within_trading_hours': True,
        'sector_industry_match': True,
        'position_already_open': False,
        'order_status': 'initiated',
        'failure_reason': None
    }

    try:
        # Check if strategy is turned on
        if strategy_config.get('On_Off', '').lower() != 'on':
            log_data['order_status'] = 'skipped'
            log_data['failure_reason'] = 'Strategy turned off'
            insert_place_order_log(engine, log_data)
            logging.info(f"Strategy {strategy_config['Strategy']} is turned off. Skipping trade for {symbol}")
            return

        # Determine symbol suffix early
        if strategy_config['instrument_type'] == 'FUT':
            symbol_suffix = f"{symbol}-Oct2024-FUT"
        elif strategy_config['instrument_type'] == 'EQ':
            symbol_suffix = symbol
        else:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = f"Unsupported instrument type {strategy_config['instrument_type']}"
            insert_place_order_log(engine, log_data)
            logging.error(f"Unsupported instrument type {strategy_config['instrument_type']} for {symbol}. Skipping.")
            return

        # Check Maxinastrategy
        maxinastrategy = strategy_config.get('Maxinastrategy')
        if maxinastrategy is not None and maxinastrategy != '':
            try:
                maxinastrategy = int(maxinastrategy)
                existing_trades = check_existing_trades(symbol_suffix, strategy_config['Strategy'], engine)
                logging.info(f"[{datetime.utcnow()}] Checking Maxinastrategy for {symbol_suffix}: limit {maxinastrategy}, existing trades {existing_trades}")
                if existing_trades >= maxinastrategy:
                    log_data['order_status'] = 'skipped'
                    log_data['failure_reason'] = f'Max trades ({maxinastrategy}) for this symbol and strategy reached'
                    insert_place_order_log(engine, log_data)
                    logging.info(f"[{datetime.utcnow()}] Skipping trade for {symbol_suffix}: Max trades for this symbol and strategy reached")
                    return
            except ValueError:
                logging.warning(f"[{datetime.utcnow()}] Invalid Maxinastrategy value: {maxinastrategy}. Ignoring this check.")

        # Rest of the function remains the same...


        

        # Check trading hours
        log_data['start_time'] = strategy_config['Start']
        log_data['stop_time'] = strategy_config['Stop']
        if not within_trading_hours(strategy_config['Start'], strategy_config['Stop']):
            log_data['within_trading_hours'] = False
            log_data['order_status'] = 'skipped'
            log_data['failure_reason'] = 'Outside trading hours'
            insert_place_order_log(engine, log_data)
            logging.info(f"Outside trading hours for {symbol}")
            return

        # Check sector and industry
        sector, industry = get_sector_and_industry(symbol)
        log_data['sector'] = sector
        log_data['industry'] = industry
        log_data['sector_in'] = strategy_config['sector_in']
        log_data['industry_in'] = strategy_config['industry_in']
        if not check_sector_industry(sector, industry, strategy_config):
            log_data['sector_industry_match'] = False
            log_data['order_status'] = 'skipped'
            log_data['failure_reason'] = 'Sector/Industry mismatch'
            insert_place_order_log(engine, log_data)
            logging.info(f"Sector/Industry mismatch for {symbol}")
            return

        # Get trading list and lots
        trading_list_df = get_trading_list()
        lots_df = get_lots()
        if trading_list_df is None or lots_df is None:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = 'Failed to fetch trading list or lots data'
            insert_place_order_log(engine, log_data)
            logging.error("Failed to fetch trading list or lots data")
            return

        # Determine symbol suffix and exchange segment
        log_data['instrument_type'] = strategy_config['instrument_type']
        if strategy_config['instrument_type'] == 'FUT':
            symbol_suffix = f"{symbol}-Oct2024-FUT"
            exchange_segment = NSE_FNO
            lot_size = lots_df.loc[lots_df['Symbol'] == symbol, 'Oct'].values[0] if not lots_df.empty else None
        elif strategy_config['instrument_type'] == 'EQ':
            symbol_suffix = symbol
            exchange_segment = NSE_EQ
            lot_size = 1
        else:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = f"Unsupported instrument type {strategy_config['instrument_type']}"
            insert_place_order_log(engine, log_data)
            logging.error(f"Unsupported instrument type {strategy_config['instrument_type']} for {symbol}. Skipping.")
            return

        if lot_size is None:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = 'Failed to determine lot size'
            insert_place_order_log(engine, log_data)
            logging.error(f"Failed to determine lot size for {symbol}")
            return

        log_data['quantity'] = lot_size
        log_data['exchange_segment'] = exchange_segment
        log_data['product_type'] = strategy_config['product_type']

        # Get security ID
        matches = trading_list_df.loc[trading_list_df['SEM_TRADING_SYMBOL'] == symbol_suffix, 'SEM_SMST_SECURITY_ID']
        if matches.empty:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = 'No match found in trading list'
            insert_place_order_log(engine, log_data)
            logging.error(f"No match found for {symbol_suffix}")
            return
        
        security_id = matches.values[0]
        log_data['security_id'] = security_id

        # Check if position is already open
        if is_position_open(symbol_suffix, strategy_config['Strategy'], engine):
            log_data['position_already_open'] = True
            log_data['order_status'] = 'skipped'
            log_data['failure_reason'] = 'Position already open'
            insert_place_order_log(engine, log_data)
            logging.info(f"Position already open for {symbol_suffix}. Skipping new order.")
            return

        # Check for max positions and position size
        today = datetime.now().strftime('%Y-%m-%d')
        with engine.connect() as connection:
            query = text("""
            SELECT COUNT(*) as today_orders_count, 
                COALESCE(SUM(position_size), 0) as total_position_size_today
            FROM trades 
            WHERE DATE(timestamp) = :today AND strategy = :strategy AND order_status = 'open'
            """)
            result = connection.execute(query, {"today": today, "strategy": strategy_config['Strategy']}).fetchone()
            today_orders_count = result[0] or 0
            total_position_size_today = result[1] or 0

        log_data['today_orders_count'] = today_orders_count
        log_data['total_position_size_today'] = total_position_size_today
        log_data['max_positions'] = strategy_config["Max_Positions"]
        log_data['max_position_size'] = strategy_config["Max_PositionSize"]

        if today_orders_count >= strategy_config["Max_Positions"]:
            log_data['order_status'] = 'skipped'
            log_data['failure_reason'] = 'Max positions limit reached'
            insert_place_order_log(engine, log_data)
            logging.info(f"Max positions limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_Positions']}. Skipping new order.")
            return

        if total_position_size_today >= strategy_config["Max_PositionSize"]:
            log_data['order_status'] = 'skipped'
            log_data['failure_reason'] = 'Max position size limit reached'
            insert_place_order_log(engine, log_data)
            logging.info(f"Max position size limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_PositionSize']}. Skipping new order.")
            return

        # Get price
        entry_price = None
        price_from_redis = get_price(security_id)
        if price_from_redis is not None:
            entry_price = price_from_redis
            logging.info(f"Using price from Redis for {symbol_suffix}: {entry_price}")
        else:
            logging.info(f"Failed to get price from Redis for {symbol_suffix}. Falling back to yfinance.")
            try:
                ticker = f'{symbol}.NS'
                data = yf.download(ticker, period='1d', interval='1m')
                if not data.empty:
                    entry_price = round(data['Close'].iloc[-1], 2)
                    logging.info(f"Using price from yfinance for {symbol_suffix}: {entry_price}")
                else:
                    raise ValueError("No data returned from yfinance")
            except Exception as e:
                log_data['order_status'] = 'error'
                log_data['failure_reason'] = f'Failed to get price: {str(e)}'
                insert_place_order_log(engine, log_data)
                logging.error(f"Error downloading data from yfinance for {ticker}: {e}")
                return

        if entry_price is None:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = 'Failed to get valid price'
            insert_place_order_log(engine, log_data)
            logging.error(f"Failed to get valid price for {symbol_suffix}")
            return

        log_data['price'] = entry_price

        # Calculate position size
        position_size = round(entry_price * lot_size, 2)
        log_data['position_size'] = position_size

        # Check Max_Stock_Position_Size
        max_stock_position_size = strategy_config.get('Max_Stock_Position_Size')
        log_data['max_stock_position_size'] = max_stock_position_size

        if max_stock_position_size is not None and max_stock_position_size != '' and float(max_stock_position_size) > 0:
            if position_size > float(max_stock_position_size):
                log_data['order_status'] = 'skipped'
                log_data['failure_reason'] = f'Position size ({position_size}) exceeds Max_Stock_Position_Size ({max_stock_position_size})'
                insert_place_order_log(engine, log_data)
                logging.info(f"Skipping trade for {symbol}: Position size {position_size} exceeds Max_Stock_Position_Size {max_stock_position_size}")
                return

        # Calculate ATR
        try:
            ticker = f'{symbol}.NS'
            data = yf.download(ticker, period='1mo', interval='1d')
            atr = round(calculate_atr(data), 2)
        except Exception as e:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = f'Failed to calculate ATR: {str(e)}'
            insert_place_order_log(engine, log_data)
            logging.error(f"Error calculating ATR for {ticker}: {e}")
            return

        if atr == 0:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = 'ATR calculation resulted in zero'
            insert_place_order_log(engine, log_data)
            logging.error(f"ATR calculation resulted in zero, skipping trade for {symbol_suffix}")
            return

        log_data['atr'] = atr
        log_data['atr_sl_multiplier'] = strategy_config["ATR_SL"]
        log_data['atr_target_multiplier'] = strategy_config["ATR_Target"]

        # Calculate stop loss and target
        if strategy_config['TradeType'] == 'Long':
            stop_loss = round((entry_price - strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price + strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        elif strategy_config['TradeType'] == 'Short':
            stop_loss = round((entry_price + strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price - strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        else:
            log_data['order_status'] = 'error'
            log_data['failure_reason'] = f"Unknown trade type {strategy_config['TradeType']}"
            insert_place_order_log(engine, log_data)
            logging.error(f"Unknown trade type {strategy_config['TradeType']} for {symbol_suffix}. Skipping.")
            return

        log_data['trade_type'] = strategy_config['TradeType']
        log_data['stop_loss'] = stop_loss
        log_data['target'] = target

        sl_percentage = round(abs((entry_price - stop_loss) / entry_price) * 100, 2)
        target_percentage = round(abs((target - entry_price) / entry_price) * 100, 2)
        max_loss = round(abs((entry_price - stop_loss) * lot_size), 2)
        max_profit = round(abs((target - entry_price) * lot_size), 2)

        log_data['sl_percentage'] = sl_percentage
        log_data['target_percentage'] = target_percentage
        log_data['max_loss'] = max_loss
        log_data['max_profit'] = max_profit

        if float(total_position_size_today) + position_size > float(strategy_config["Max_PositionSize"]):
            log_data['order_status'] = 'skipped'
            log_data['failure_reason'] = 'Adding this position would exceed Max_PositionSize'
            insert_place_order_log(engine, log_data)
            logging.info(f"Adding this position would exceed Max_PositionSize for strategy {strategy_config['Strategy']}. Skipping trade.")
            return

        log_data['holding_period'] = strategy_config['Holding_Period']
        log_data['cycle_time_in_mins'] = strategy_config.get('Cycle_time_in_mins')

        # Place order
        response = place_order(dhan, symbol_suffix, security_id, lot_size, entry_price, stop_loss, target, 
                               strategy_config['TradeType'], strategy_config['Strategy'], 
                               strategy_config['product_type'], exchange_segment, 
                               strategy_config['Holding_Period'],
                               strategy_config.get('Cycle_time_in_mins'))
        
        if response and response.get('status') == 'success':
            log_data['order_status'] = 'success'
            log_data['response'] = json.dumps(response)
            logging.info(f"{strategy_config['TradeType']} order executed for {symbol_suffix}: {response}")
        else:
            log_data['order_status'] = 'failed'
            log_data['failure_reason'] = 'Order placement failed'
            log_data['response'] = json.dumps(response) if response else None
            logging.error(f"Failed to execute {strategy_config['TradeType']} order for {symbol_suffix}. Response: {response}")

        # Insert the log data into the place_order table
        insert_place_order_log(engine, log_data)

    except Exception as e:
        logging.error(f"Error in process_trade for symbol {symbol}: {str(e)}")
        logging.error(f"Full error details: {traceback.format_exc()}")
        log_data['order_status'] = 'error'
        log_data['failure_reason'] = f'Unexpected error: {str(e)}'
        insert_place_order_log(engine, log_data)


def process_alert(alert_data):
    try:
        strategy_name = alert_data['alert_name']
        symbols = alert_data['stocks'].split(',')
        strategy_config = get_strategy_config(strategy_name)
        if strategy_config is None:
            logging.error(f"No strategy configuration found for {strategy_name}")
            return
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        for symbol in symbols:
            process_trade(dhan, symbol, strategy_config)
    except Exception as e:
        logging.error(f"Error in process_alert: {str(e)}")

def check_redis_connection():
    if redis_client is None:
        logging.error("Redis client is not initialized.")
        return False
    try:
        redis_client.ping()
        logging.info("Successfully connected to Redis")
        return True
    except redis.ConnectionError as e:
        logging.error(f"Failed to connect to Redis: {e}")
        return False

if __name__ == "__main__":
    if check_redis_connection():
        logging.info("Starting trading logic...")
        # Here you would typically set up your main loop or event handler
        # For example, you might listen for incoming alerts and process them
        # process_alert(some_alert_data)
    else:
        logging.error("Exiting due to Redis connection failure")
