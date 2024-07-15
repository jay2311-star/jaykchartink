import pandas as pd
import os
import sqlite3
import json
import requests
from dhanhq import dhanhq
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import traceback
import redis
import pytz



ist = pytz.timezone('Asia/Kolkata')

def get_ist_timestamp():
    return datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')





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

# Dhan API client credentials
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')

# Redis connection details
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# Create a Redis client
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_db_connection():
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        return engine
    except Exception as e:
        print(f"Error while connecting to MySQL: {e}")
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
                # Ensure Holding_Period and Cycle_time_in_mins are included
                if 'Holding_Period' not in config:
                    config['Holding_Period'] = 'day'  # Default to 'day' if not specified
                if 'Cycle_time_in_mins' not in config:
                    config['Cycle_time_in_mins'] = None  # Default to None if not specified
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
            print(f"Error while fetching trading list: {e}")
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
            print(f"Error while fetching lots: {e}")
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
            print(f"Error while fetching sector and industry for {symbol_ns}: {e}")
            return "Unknown", "Unknown"
        finally:
            if engine is not None:
                engine.dispose()
    else:
        return "Unknown", "Unknown"

def log_entry(message, level="INFO"):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - {level} - {message}")

def save_trade_log_to_mysql(trade_entries):
    if not trade_entries:
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
                        print(f"Duplicate entry found for security_id {entry['security_id']} at {entry['timestamp']}. Skipping insertion.")
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
                except Exception as e:
                    print(f"Error inserting entry into MySQL: {e}")
                    print(f"Problematic entry: {entry}")
            
            connection.commit()
            print("Trade log appended to MySQL database successfully.")
        except Exception as e:
            print(f"Error while saving trade log to database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
            engine.dispose()
    else:
        print("Failed to connect to the database. Trade log not saved.")

def get_price(security_id):
    try:
        price_data = redis_client.hgetall(f"price:{security_id}")
        if price_data and 'latest_price' in price_data:
            return float(price_data['latest_price'])
        logging.warning(f"No price data found for security ID {security_id} in Redis. Available data: {price_data}")
        return None
    except redis.RedisError as e:
        logging.error(f"Redis error while fetching price for security ID {security_id}: {e}")
        return None
    except ValueError as e:
        logging.error(f"Invalid price data for security ID {security_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error while fetching price for security ID {security_id}: {e}")
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
        log_entry("=" * 50)
        log_entry(f"Attempting to place order for {symbol}")
        log_entry(f"Order details:")
        log_entry(f"  Security ID: {security_id}")
        log_entry(f"  Symbol: {symbol}")
        log_entry(f"  Quantity: {lot_size}")
        log_entry(f"  Entry Price: {entry_price}")
        log_entry(f"  Stop Loss: {stop_loss}")
        log_entry(f"  Target: {target}")
        log_entry(f"  Trade Type: {trade_type}")
        log_entry(f"  Product Type: {product_type}")
        log_entry(f"  Strategy Key: {strategy_key}")
        log_entry(f"  Exchange Segment: {exchange_segment}")
        log_entry(f"  Holding Period: {holding_period}")
        log_entry(f"  Cycle Time in Minutes: {cycle_time_in_mins}")

        transaction_type = BUY if trade_type == 'Long' else SELL

        log_entry("Dhan API call parameters:")
        log_entry(f"  security_id: {str(security_id)}")
        log_entry(f"  exchange_segment: {exchange_segment}")
        log_entry(f"  transaction_type: {transaction_type}")
        log_entry(f"  quantity: {lot_size}")
        log_entry(f"  order_type: {MARKET}")
        log_entry(f"  product_type: {product_type}")
        log_entry(f"  price: 0")
        log_entry(f"  tag: {strategy_key}")

        log_entry("Calling Dhan place_order API...")
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
        log_entry(f"Dhan API Response:")
        log_entry(json.dumps(response, indent=2))

        if response and response['status'] == 'success':
            log_entry("Order placement successful")
            
            # Calculate planned_exit_datetime
            entry_datetime = datetime.now(ist)
            planned_exit = None
            
            log_entry(f"Calculating planned exit for holding period: {holding_period}")
            
            if holding_period.lower() == 'minute':
                if cycle_time_in_mins is not None:
                    minutes_to_add = int(cycle_time_in_mins) * 5
                    planned_exit = entry_datetime + timedelta(minutes=minutes_to_add)
                    log_entry(f"Using Cycle_time_in_mins: {cycle_time_in_mins}, adding {minutes_to_add} minutes")
                else:
                    log_entry("Cycle_time_in_mins not provided for 'minute' holding period. Using default of 5 minutes.", "WARNING")
                    planned_exit = entry_datetime + timedelta(minutes=5)
            elif holding_period.lower() == 'day':
                planned_exit = entry_datetime.date() + timedelta(days=1)
            elif holding_period.lower() == 'week':
                planned_exit = entry_datetime.date() + timedelta(days=5)
            elif holding_period.lower() == 'month':
                planned_exit = entry_datetime.date() + timedelta(days=20)
            else:
                log_entry(f"Unknown holding period '{holding_period}' for trade {symbol}. Using default of 1 day.", "WARNING")
                planned_exit = entry_datetime.date() + timedelta(days=1)

            planned_exit_datetime = None
            if planned_exit:
                if isinstance(planned_exit, datetime):
                    planned_exit_datetime = planned_exit
                else:
                    planned_exit_datetime = datetime.combine(planned_exit, time(15, 15))
                planned_exit_datetime = ist.localize(planned_exit_datetime)
                
                # Ensure planned exit is not in the past
                if planned_exit_datetime <= entry_datetime:
                    planned_exit_datetime += timedelta(days=1)
                    log_entry(f"Adjusted planned exit to next day: {planned_exit_datetime}", "WARNING")

            log_entry(f"Calculated planned exit datetime: {planned_exit_datetime}")

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
                'atr_sl_multiplier': None,  # Add this if available
                'atr_target_multiplier': None,  # Add this if available
                'product_type': product_type,
                'position_size': entry_price * lot_size,
                'holding_period': holding_period,
                'exit_time': None,
                'realized_profit': None,
                'planned_exit_datetime': planned_exit_datetime,
                'exit_reason': None
            }
            
            log_entry("Saving trade entry to database...")
            save_trade_log_to_mysql([trade_entry])
            log_entry("Trade entry saved successfully")
        else:
            log_entry("Order placement failed", "ERROR")
        
        log_entry("=" * 50)
        return response
    except Exception as e:
        log_entry(f"An error occurred while placing order: {str(e)}", "ERROR")
        log_entry(f"Error details: {traceback.format_exc()}", "ERROR")
        return None
        
def get_positions(dhan):
    try:
        log_entry("Retrieving current positions")
        response = dhan.get_positions()
        if response['status'] == 'success':
            log_entry("Positions retrieved successfully")
            return response['data']
        else:
            log_entry(f"Failed to retrieve positions: {response['remarks']}", "ERROR")
            return []
    except Exception as e:
        log_entry(f"An error occurred while retrieving portfolio positions: {e}", "ERROR")
        return []

def is_position_open(symbol, positions, exchange_segment):
    for position in positions:
        if position['tradingSymbol'] == symbol and position['exchangeSegment'] == exchange_segment:
            return True
    return False

def process_trade(dhan, symbol, strategy_config):
    try:
        if strategy_config.get('On_Off', '').lower() != 'on':
            log_entry(f"Strategy {strategy_config['Strategy']} is turned off. Skipping trade for {symbol}")
            return

        if not within_trading_hours(strategy_config['Start'], strategy_config['Stop']):
            log_entry(f"Outside trading hours for {symbol}")
            return


        sector, industry = get_sector_and_industry(symbol)
        if not check_sector_industry(sector, industry, strategy_config):
            log_entry(f"Sector/Industry mismatch for {symbol}")
            return

        trading_list_df = get_trading_list()
        lots_df = get_lots()
        if trading_list_df is None or lots_df is None:
            log_entry("Failed to fetch trading list or lots data")
            return

        if strategy_config['instrument_type'] == 'FUT':
            symbol_suffix = f"{symbol}-Jul2024-FUT"
            exchange_segment = NSE_FNO
            lot_size = lots_df.loc[lots_df['Symbol'] == symbol, 'Jul'].values[0] if not lots_df.empty else None
        elif strategy_config['instrument_type'] == 'EQ':
            symbol_suffix = symbol
            exchange_segment = NSE_EQ
            lot_size = 1
        else:
            log_entry(f"Unsupported instrument type {strategy_config['instrument_type']} for {symbol}. Skipping.")
            return

        if lot_size is None:
            log_entry(f"Failed to determine lot size for {symbol}")
            return

        log_entry(f"Processing {strategy_config['TradeType']} for {symbol_suffix} with product type {strategy_config['product_type']}")
        
        matches = trading_list_df.loc[trading_list_df['SEM_TRADING_SYMBOL'] == symbol_suffix, 'SEM_SMST_SECURITY_ID']
        if matches.empty:
            log_entry(f"No match found for {symbol_suffix}")
            return
        security_id = matches.values[0]

        # Check if position is already open
        positions = get_positions(dhan)
        if is_position_open(symbol_suffix, positions, exchange_segment):
            log_entry(f"Position already open for {symbol_suffix}. Skipping new order.")
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
                today_orders_count = result[0] or 0
                total_position_size_today = result[1] or 0
            finally:
                cursor.close()
                connection.close()
                engine.dispose()

        log_entry(f"Today's orders count for strategy {strategy_config['Strategy']}: {today_orders_count}")
        log_entry(f"Today's total position size for strategy {strategy_config['Strategy']}: {total_position_size_today}")

        if today_orders_count >= strategy_config["Max_Positions"]:
            log_entry(f"Max positions limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_Positions']}. Skipping new order.")
            return

        if total_position_size_today >= strategy_config["Max_PositionSize"]:
            log_entry(f"Max position size limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_PositionSize']}. Skipping new order.")
            return

        price_from_redis = get_price(security_id)
        if price_from_redis is not None:
            entry_price = price_from_redis
            log_entry(f"Using price from Redis for {symbol_suffix}: {entry_price}")
        else:
            log_entry(f"Failed to get price from Redis for {symbol_suffix}. Falling back to yfinance.")
            try:
                ticker = f'{symbol}.NS'
                data = yf.download(ticker, period='1d', interval='1m')
                entry_price = round(data['Close'].iloc[-1], 2)
                log_entry(f"Using price from yfinance for {symbol_suffix}: {entry_price}")
            except Exception as e:
                log_entry(f"Error downloading data from yfinance for {ticker}: {e}", "ERROR")
                return

        # Calculate ATR
        try:
            ticker = f'{symbol}.NS'
            data = yf.download(ticker, period='1mo', interval='1d')
            atr = round(calculate_atr(data), 2)
        except Exception as e:
            log_entry(f"Error downloading data from yfinance for {ticker}: {e}", "ERROR")
            log_entry(f"Error calculating ATR for {ticker}: {e}", "ERROR")
            return

        if atr == 0:
            log_entry(f"ATR calculation resulted in zero, skipping trade for {symbol_suffix}", "ERROR")
            return

        if strategy_config['TradeType'] == 'Long':
            stop_loss = round((entry_price - strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price + strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        elif strategy_config['TradeType'] == 'Short':
            stop_loss = round((entry_price + strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price - strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        else:
            log_entry(f"Unknown trade type {strategy_config['TradeType']} for {symbol_suffix}. Skipping.", "ERROR")
            return

        sl_percentage = round(abs((entry_price - stop_loss) / entry_price) * 100, 2)
        target_percentage = round(abs((target - entry_price) / entry_price) * 100, 2)
        max_loss = round(abs((entry_price - stop_loss) * lot_size), 2)
        max_profit = round(abs((target - entry_price) * lot_size), 2)
        position_size = round(entry_price * lot_size, 2)

        log_entry(f"Strategy: {strategy_config['Strategy']}")
        log_entry(f"ATR: {atr}, ATR SL Multiplier: {strategy_config['ATR_SL']}, ATR Target Multiplier: {strategy_config['ATR_Target']}")
        log_entry(f"Entry Price: {entry_price}, Stop Loss: {stop_loss}, Stop Loss %: {sl_percentage}%, Target: {target}, Target %: {target_percentage}%")
        log_entry(f"Max Loss: {max_loss}, Max Profit: {max_profit}")
        log_entry(f"Lot Size: {lot_size}, Position Size: {position_size}")

        response = place_order(dhan, symbol_suffix, security_id, lot_size, entry_price, stop_loss, target, 
                               strategy_config['TradeType'], strategy_config['Strategy'], 
                               strategy_config['product_type'], exchange_segment, 
                               strategy_config['Holding_Period'],
                               strategy_config.get('Cycle_time_in_mins'))
        if response and response['status'] == 'success':
            log_entry(f"{strategy_config['TradeType']} order executed for {symbol_suffix}: {response}")
        else:
            log_entry(f"Failed to execute {strategy_config['TradeType']} order for {symbol_suffix}: {response['remarks']}", "ERROR")
            
    except Exception as e:
        log_entry(f"Error in process_trade for symbol {symbol}: {str(e)}")
        log_entry(f"Full error details: {traceback.format_exc()}")


def process_alert(alert_data):
    try:
        strategy_name = alert_data['alert_name']
        symbols = alert_data['stocks'].split(',')
        strategy_config = get_strategy_config(strategy_name)
        if strategy_config is None:
            log_entry(f"No strategy configuration found for {strategy_name}")
            return
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        for symbol in symbols:
            process_trade(dhan, symbol, strategy_config)
    except Exception as e:
        log_entry(f"Error in process_alert: {str(e)}")

def check_redis_connection():
    try:
        redis_client.ping()
        log_entry("Successfully connected to Redis")
        return True
    except redis.ConnectionError:
        log_entry("Failed to connect to Redis", "ERROR")
        return False

if __name__ == "__main__":
    if check_redis_connection():
        main()
    else:
        log_entry("Exiting due to Redis connection failure", "ERROR")