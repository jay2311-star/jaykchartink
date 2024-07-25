import pandas as pd
import os
import json
import requests
from dhanhq import dhanhq
import yfinance as yf
from datetime import datetime, timedelta, time
from sqlalchemy import create_engine
import traceback
import redis
import logging
from redis import Redis, ConnectionError, RedisError
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_BASE_URL = "http://139.59.70.202:5000"  # Replace with your droplet's IP if different

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
                # Ensure Holding_Period, Cycle_time_in_mins, and Max_Stock_Position_Size are included
                if 'Holding_Period' not in config:
                    config['Holding_Period'] = 'day'  # Default to 'day' if not specified
                if 'Cycle_time_in_mins' not in config:
                    config['Cycle_time_in_mins'] = None  # Default to None if not specified
                if 'Max_Stock_Position_Size' not in config:
                    config['Max_Stock_Position_Size'] = None  # Default to None if not specified
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

def save_order_attempt_to_mysql(order_entry):
    engine = get_db_connection()
    if engine is not None:
        try:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            
            insert_query = """
            INSERT INTO place_order (
                timestamp, symbol, strategy, security_id, quantity, price, order_type,
                transaction_type, product_type, exchange_segment, order_status, response,
                failure_reason, trade_type, stop_loss, target, position_size,
                holding_period, cycle_time_in_mins, sector, industry, sector_in,
                industry_in, instrument_type, start_time, stop_time, max_positions,
                max_position_size, max_stock_position_size, atr, atr_sl_multiplier,
                atr_target_multiplier, sl_percentage, target_percentage, max_loss,
                max_profit, today_orders_count, total_position_size_today,
                within_trading_hours, sector_industry_match, position_already_open
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            """
            cursor.execute(insert_query, (
                order_entry['timestamp'], order_entry['symbol'], order_entry['strategy'],
                order_entry['security_id'], order_entry['quantity'], order_entry['price'],
                order_entry['order_type'], order_entry['transaction_type'], order_entry['product_type'],
                order_entry['exchange_segment'], order_entry['order_status'], json.dumps(order_entry['response']),
                order_entry['failure_reason'], order_entry['trade_type'], order_entry['stop_loss'],
                order_entry['target'], order_entry['position_size'], order_entry['holding_period'],
                order_entry['cycle_time_in_mins'], order_entry['sector'], order_entry['industry'],
                order_entry['sector_in'], order_entry['industry_in'], order_entry['instrument_type'],
                order_entry['start_time'], order_entry['stop_time'], order_entry['max_positions'],
                order_entry['max_position_size'], order_entry['max_stock_position_size'],
                order_entry['atr'], order_entry['atr_sl_multiplier'], order_entry['atr_target_multiplier'],
                order_entry['sl_percentage'], order_entry['target_percentage'], order_entry['max_loss'],
                order_entry['max_profit'], order_entry['today_orders_count'],
                order_entry['total_position_size_today'], order_entry['within_trading_hours'],
                order_entry['sector_industry_match'], order_entry['position_already_open']
            ))
            
            connection.commit()
            logging.info(f"Order attempt for {order_entry['symbol']} saved to place_order table")
        except Exception as e:
            logging.error(f"Error saving order attempt to place_order table: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
            engine.dispose()
    else:
        logging.error("Failed to connect to the database. Order attempt not saved.")

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

def place_order(dhan, symbol, security_id, lot_size, entry_price, stop_loss, target, trade_type, strategy_key, product_type, exchange_segment, holding_period, cycle_time_in_mins, order_entry):
    try:
        logging.info("=" * 50)
        logging.info(f"Attempting to place order for {symbol}")
        
        transaction_type = BUY if trade_type == 'Long' else SELL
        
        order_entry.update({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'strategy': strategy_key,
            'security_id': security_id,
            'quantity': lot_size,
            'price': entry_price,
            'order_type': MARKET,
            'transaction_type': transaction_type,
            'product_type': product_type,
            'exchange_segment': exchange_segment,
            'order_status': 'Pending',
            'response': None,
            'failure_reason': None,
            'trade_type': trade_type,
            'stop_loss': stop_loss,
            'target': target,
            'position_size': entry_price * lot_size,
            'holding_period': holding_period,
            'cycle_time_in_mins': cycle_time_in_mins
        })

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

        order_entry['response'] = response
        if response and response['status'] == 'success':
            order_entry['order_status'] = 'Success'
            logging.info("Order placement successful")
        else:
            order_entry['order_status'] = 'Failed'
            order_entry['failure_reason'] = response.get('remarks', 'Unknown error')
            logging.error("Order placement failed")
        
        # Save the order attempt to the place_order table
        save_order_attempt_to_mysql(order_entry)
        
        logging.info("=" * 50)
        return response
    except Exception as e:
        logging.error(f"An error occurred while placing order: {str(e)}")
        logging.error(f"Error details: {traceback.format_exc()}")
        
        order_entry['order_status'] = 'Error'
        order_entry['failure_reason'] = str(e)
        save_order_attempt_to_mysql(order_entry)
        
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

def is_position_open(symbol, positions, exchange_segment):
    for position in positions:
        if position['tradingSymbol'] == symbol and position['exchangeSegment'] == exchange_segment:
            return True
    return False

def process_trade(dhan, symbol, strategy_config):
    try:
        order_entry = {
            'strategy': strategy_config['Strategy'],
            'sector_in': strategy_config['sector_in'],
            'industry_in': strategy_config['industry_in'],
            'instrument_type': strategy_config['instrument_type'],
            'start_time': strategy_config['Start'],
            'stop_time': strategy_config['Stop'],
            'max_positions': strategy_config['Max_Positions'],
            'max_position_size': strategy_config['Max_PositionSize'],
            'max_stock_position_size': strategy_config.get('Max_Stock_Position_Size'),

            ##
            'atr_sl_multiplier': strategy_config['ATR_SL'],
            'atr_target_multiplier': strategy_config['ATR_Target'],
            'within_trading_hours': False,
            'sector_industry_match': False,
            'position_already_open': False
        }

        if strategy_config.get('On_Off', '').lower() != 'on':
            logging.info(f"Strategy {strategy_config['Strategy']} is turned off. Skipping trade for {symbol}")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'Strategy turned off'
            save_order_attempt_to_mysql(order_entry)
            return

        order_entry['within_trading_hours'] = within_trading_hours(strategy_config['Start'], strategy_config['Stop'])
        if not order_entry['within_trading_hours']:
            logging.info(f"Outside trading hours for {symbol}")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'Outside trading hours'
            save_order_attempt_to_mysql(order_entry)
            return

        sector, industry = get_sector_and_industry(symbol)
        order_entry['sector'] = sector
        order_entry['industry'] = industry
        order_entry['sector_industry_match'] = check_sector_industry(sector, industry, strategy_config)
        if not order_entry['sector_industry_match']:
            logging.info(f"Sector/Industry mismatch for {symbol}")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'Sector/Industry mismatch'
            save_order_attempt_to_mysql(order_entry)
            return

        trading_list_df = get_trading_list()
        lots_df = get_lots()
        if trading_list_df is None or lots_df is None:
            logging.error("Failed to fetch trading list or lots data")
            order_entry['order_status'] = 'Error'
            order_entry['failure_reason'] = 'Failed to fetch trading list or lots data'
            save_order_attempt_to_mysql(order_entry)
            return

        if strategy_config['instrument_type'] == 'FUT':
            symbol_suffix = f"{symbol}-Aug2024-FUT"
            exchange_segment = NSE_FNO
            lot_size = lots_df.loc[lots_df['Symbol'] == symbol, 'Aug'].values[0] if not lots_df.empty else None
        elif strategy_config['instrument_type'] == 'EQ':
            symbol_suffix = symbol
            exchange_segment = NSE_EQ
            lot_size = 1
        else:
            logging.error(f"Unsupported instrument type {strategy_config['instrument_type']} for {symbol}. Skipping.")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = f"Unsupported instrument type: {strategy_config['instrument_type']}"
            save_order_attempt_to_mysql(order_entry)
            return

        if lot_size is None:
            logging.error(f"Failed to determine lot size for {symbol}")
            order_entry['order_status'] = 'Error'
            order_entry['failure_reason'] = 'Failed to determine lot size'
            save_order_attempt_to_mysql(order_entry)
            return

        logging.info(f"Processing {strategy_config['TradeType']} for {symbol_suffix} with product type {strategy_config['product_type']}")
        
        matches = trading_list_df.loc[trading_list_df['SEM_TRADING_SYMBOL'] == symbol_suffix, 'SEM_SMST_SECURITY_ID']
        if matches.empty:
            logging.error(f"No match found for {symbol_suffix}")
            order_entry['order_status'] = 'Error'
            order_entry['failure_reason'] = f"No match found for {symbol_suffix}"
            save_order_attempt_to_mysql(order_entry)
            return
        security_id = matches.values[0]

        # Check if position is already open
        positions = get_positions(dhan)
        order_entry['position_already_open'] = is_position_open(symbol_suffix, positions, exchange_segment)
        if order_entry['position_already_open']:
            logging.info(f"Position already open for {symbol_suffix}. Skipping new order.")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'Position already open'
            save_order_attempt_to_mysql(order_entry)
            return

        # Check for max positions
        today = datetime.now().strftime('%Y-%m-%d')
        engine = get_db_connection()
        if engine is not None:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            try:
                query = """
                SELECT COUNT(*) as today_orders_count, 
                    SUM(position_size) as total_position_size_today
                FROM trades 
                WHERE DATE(timestamp) = %s AND strategy = %s AND order_status = 'open'
                """
                cursor.execute(query, (today, strategy_config['Strategy']))
                result = cursor.fetchone()
                order_entry['today_orders_count'] = result[0] or 0
                order_entry['total_position_size_today'] = result[1] or 0
            finally:
                cursor.close()
                connection.close()
                engine.dispose()

        logging.info(f"Today's orders count for strategy {strategy_config['Strategy']}: {order_entry['today_orders_count']}")
        logging.info(f"Today's total position size for strategy {strategy_config['Strategy']}: {order_entry['total_position_size_today']}")

        if order_entry['today_orders_count'] >= strategy_config["Max_Positions"]:
            logging.info(f"Max positions limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_Positions']}. Skipping new order.")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'Max positions limit reached'
            save_order_attempt_to_mysql(order_entry)
            return

        if order_entry['total_position_size_today'] >= strategy_config["Max_PositionSize"]:
            logging.info(f"Max position size limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_PositionSize']}. Skipping new order.")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'Max position size limit reached'
            save_order_attempt_to_mysql(order_entry)
            return

        price_from_redis = get_price(security_id)
        if price_from_redis is not None:
            entry_price = price_from_redis
            logging.info(f"Using price from Redis for {symbol_suffix}: {entry_price}")
        else:
            logging.info(f"Failed to get price from Redis for {symbol_suffix}. Falling back to yfinance.")
            try:
                ticker = f'{symbol}.NS'
                data = yf.download(ticker, period='1d', interval='1m')
                entry_price = round(data['Close'].iloc[-1], 2)
                logging.info(f"Using price from yfinance for {symbol_suffix}: {entry_price}")
            except Exception as e:
                logging.error(f"Error downloading data from yfinance for {ticker}: {e}")
                order_entry['order_status'] = 'Error'
                order_entry['failure_reason'] = f"Failed to get price: {str(e)}"
                save_order_attempt_to_mysql(order_entry)
                return

        # Calculate ATR
        try:
            ticker = f'{symbol}.NS'
            data = yf.download(ticker, period='1mo', interval='1d')
            atr = round(calculate_atr(data), 2)
            order_entry['atr'] = atr
        except Exception as e:
            logging.error(f"Error downloading data from yfinance for {ticker}: {e}")
            logging.error(f"Error calculating ATR for {ticker}: {e}")
            order_entry['order_status'] = 'Error'
            order_entry['failure_reason'] = f"Failed to calculate ATR: {str(e)}"
            save_order_attempt_to_mysql(order_entry)
            return

        if atr == 0:
            logging.error(f"ATR calculation resulted in zero, skipping trade for {symbol_suffix}")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'ATR calculation resulted in zero'
            save_order_attempt_to_mysql(order_entry)
            return

        if strategy_config['TradeType'] == 'Long':
            stop_loss = round((entry_price - strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price + strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        elif strategy_config['TradeType'] == 'Short':
            stop_loss = round((entry_price + strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price - strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        else:
            logging.error(f"Unknown trade type {strategy_config['TradeType']} for {symbol_suffix}. Skipping.")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = f"Unknown trade type: {strategy_config['TradeType']}"
            save_order_attempt_to_mysql(order_entry)
            return

        sl_percentage = round(abs((entry_price - stop_loss) / entry_price) * 100, 2)
        target_percentage = round(abs((target - entry_price) / entry_price) * 100, 2)
        max_loss = round(abs((entry_price - stop_loss) * lot_size), 2)
        max_profit = round(abs((target - entry_price) * lot_size), 2)
        position_size = round(entry_price * lot_size, 2)

        order_entry.update({
            'stop_loss': stop_loss,
            'target': target,
            'sl_percentage': sl_percentage,
            'target_percentage': target_percentage,
            'max_loss': max_loss,
            'max_profit': max_profit,
            'position_size': position_size
        })

        # Check for Max_Stock_Position_Size constraint
        max_stock_position_size = strategy_config.get('Max_Stock_Position_Size')
        if max_stock_position_size is not None and position_size > max_stock_position_size:
            logging.info(f"Position size {position_size} exceeds Max_Stock_Position_Size {max_stock_position_size} for {symbol_suffix}. Adjusting lot size.")
            adjusted_lot_size = int(max_stock_position_size / entry_price)
            if adjusted_lot_size < 1:
                logging.info(f"Adjusted lot size would be less than 1 for {symbol_suffix}. Skipping trade.")
                order_entry['order_status'] = 'Skipped'
                order_entry['failure_reason'] = 'Adjusted lot size less than 1'
                save_order_attempt_to_mysql(order_entry)
                return
            lot_size = adjusted_lot_size
            position_size = round(entry_price * lot_size, 2)
            max_loss = round(abs((entry_price - stop_loss) * lot_size), 2)
            max_profit = round(abs((target - entry_price) * lot_size), 2)
            order_entry.update({
                'quantity': lot_size,
                'position_size': position_size,
                'max_loss': max_loss,
                'max_profit': max_profit
            })
            logging.info(f"Adjusted lot size to {lot_size}, new position size: {position_size}")

        logging.info(f"Strategy: {strategy_config['Strategy']}")
        logging.info(f"ATR: {atr}, ATR SL Multiplier: {strategy_config['ATR_SL']}, ATR Target Multiplier: {strategy_config['ATR_Target']}")
        logging.info(f"Entry Price: {entry_price}, Stop Loss: {stop_loss}, Stop Loss %: {sl_percentage}%, Target: {target}, Target %: {target_percentage}%")
        logging.info(f"Max Loss: {max_loss}, Max Profit: {max_profit}")
        logging.info(f"Lot Size: {lot_size}, Position Size: {position_size}")

        # Check if the adjusted position size still fits within the overall strategy limits
        if float(order_entry['total_position_size_today']) + position_size > float(strategy_config["Max_PositionSize"]):
            logging.info(f"Adding this position would exceed Max_PositionSize for strategy {strategy_config['Strategy']}. Skipping trade.")
            order_entry['order_status'] = 'Skipped'
            order_entry['failure_reason'] = 'Would exceed Max_PositionSize'
            save_order_attempt_to_mysql(order_entry)
            return

        response = place_order(dhan, symbol_suffix, security_id, lot_size, entry_price, stop_loss, target, 
                            strategy_config['TradeType'], strategy_config['Strategy'], 
                            strategy_config['product_type'], exchange_segment, 
                            strategy_config['Holding_Period'],
                            strategy_config.get('Cycle_time_in_mins'),
                            order_entry)
        if response and response.get('status') == 'success':
            logging.info(f"{strategy_config['TradeType']} order executed for {symbol_suffix}: {response}")
        else:
            logging.error(f"Failed to execute {strategy_config['TradeType']} order for {symbol_suffix}. Response: {response}")
    except Exception as e:
        logging.error(f"Error in process_trade for symbol {symbol}: {str(e)}")
        logging.error(f"Full error details: {traceback.format_exc()}")
        order_entry['order_status'] = 'Error'
        order_entry['failure_reason'] = str(e)
        save_order_attempt_to_mysql(order_entry)

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
        # Here you would typically have your main execution logic
        # For example, you might set up a loop to continuously check for new alerts
        # or implement a scheduler to run trades at specific times
        
        # Example of processing a single alert (for demonstration purposes)
        example_alert = {
            'alert_name': 'ExampleStrategy',
            'stocks': 'SBIN,RELIANCE,TCS'
        }
        process_alert(example_alert)
        
        # In a real-world scenario, you might have something like:
        # while True:
        #     new_alerts = check_for_new_alerts()  # You'd need to implement this function
        #     for alert in new_alerts:
        #         process_alert(alert)
        #     time.sleep(60)  # Wait for 1 minute before checking again
    else:
        logging.error("Exiting due to Redis connection failure")

# Additional helper functions can be added here as needed

def check_for_new_alerts():
    # This is a placeholder function
    # In a real implementation, this would check for new alerts from your alert system
    # For example, it might query a database or an API
    logging.info("Checking for new alerts...")
    return []  # Return an empty list for now
