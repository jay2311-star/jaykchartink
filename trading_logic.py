import pandas as pd
import os
import sqlite3
import json
import requests
from dhanhq import dhanhq
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine

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
client_id = '1100526168'
access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzIyNzU4NzU0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiaHR0cHM6Ly93dmY3eDdxYnMyLmV4ZWN1dGUtYXBpLnVzLWVhc3QtMS5hbWF6b25hd3MuY29tL2RldnRlc3QvcG9zdGJhY2siLCJkaGFuQ2xpZW50SWQiOiIxMTAyODM5ODc2In0.zKIqHmjdK6qmEXEcxcef0ESCgME68lE1NM6Q6pLIL-8wzw8OMhkKwE9VfUjR3Ll6_jiMhGKrZDNVWAvuPTD9hQ'

def get_db_connection():
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        return engine
    except Error as e:
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
        except Error as e:
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

def save_summary_to_db(summary_entries):
    engine = get_db_connection()
    if engine is not None:
        try:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            
            for entry in summary_entries:
                run_date = datetime.strptime(entry['run_date'], '%d-%b').strftime('%Y-%m-%d')
                
                check_query = """
                SELECT * FROM StrategySummary 
                WHERE run_date = %s AND strategy_name = %s
                """
                cursor.execute(check_query, (run_date, entry['strategy_name']))
                existing_entry = cursor.fetchone()
                
                if existing_entry:
                    update_query = """
                    UPDATE StrategySummary SET
                    run_time = %s,
                    no_of_symbols_fetched = %s,
                    trade_type = %s,
                    max_profit = %s,
                    max_loss = %s,
                    total_position_size = %s,
                    position_size_placed = %s,
                    orders_placed = %s
                    WHERE run_date = %s AND strategy_name = %s
                    """
                    cursor.execute(update_query, (
                        entry['run_time'],
                        entry['no_of_symbols_fetched'],
                        entry['trade_type'],
                        entry['max_profit'],
                        entry['max_loss'],
                        entry['total_position_size'],
                        entry['position_size_placed'],
                        entry['orders_placed'],
                        run_date,
                        entry['strategy_name']
                    ))
                else:
                    insert_query = """
                    INSERT INTO StrategySummary (
                        run_date, run_time, strategy_name, no_of_symbols_fetched,
                        trade_type, max_profit, max_loss, total_position_size,
                        position_size_placed, orders_placed
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        run_date,
                        entry['run_time'],
                        entry['strategy_name'],
                        entry['no_of_symbols_fetched'],
                        entry['trade_type'],
                        entry['max_profit'],
                        entry['max_loss'],
                        entry['total_position_size'],
                        entry['position_size_placed'],
                        entry['orders_placed']
                    ))
            
            connection.commit()
            print(f"Strategy summary saved to database successfully.")
        except Error as e:
            print(f"Error while saving strategy summary to database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
            engine.dispose()
    else:
        print("Failed to connect to the database. Strategy summary not saved.")

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
                    entry.pop('error_message', None)

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
                        atr_target_multiplier, product_type, position_size, holding_period
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        entry['timestamp'], entry['symbol'], entry['strategy'], entry['action'], entry['security_id'],
                        entry['quantity'], entry['price'], entry['order_type'], entry['trigger_price'], entry['entry_price'],
                        entry['exit_price'], entry['stop_loss'], entry['target'], entry['order_status'], entry['response'],
                        entry['max_profit'], entry['max_loss'], entry['trade_type'], entry['stop_loss_percentage'],
                        entry['target_percentage'], entry['atr_sl_multiplier'], entry['atr_target_multiplier'],
                        entry['product_type'], entry['position_size'], entry['holding_period']
                    ))
                except Exception as e:
                    print(f"Error inserting entry into MySQL: {e}")
                    print(f"Problematic entry: {entry}")
            
            connection.commit()
            print("Trade log appended to MySQL database successfully.")
        except Error as e:
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
        for endpoint in ['prices', 'historical_prices', 'prices1']:  
            url = f'http://ec2-54-242-226-103.compute-1.amazonaws.com:8000/{endpoint}'
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, dict) and str(security_id) in data:
                        price_info = data[str(security_id)]
                        if isinstance(price_info, dict):
                            latest_price = price_info.get('latest_price')
                            if latest_price is not None:
                                print(f"Price for security ID {security_id} found in {endpoint}")
                                return latest_price
                        elif isinstance(price_info, list) and price_info:
                            latest_price = price_info[0].get('price')
                            if latest_price is not None:
                                print(f"Price for security ID {security_id} found in {endpoint}")
                                return latest_price
                else:
                    print(f"Failed to fetch {endpoint} from server. Status code: {response.status_code}")
            except requests.RequestException as e:
                print(f"Error fetching the {endpoint} from server: {e}")
        print(f"No price data available for security ID {security_id} in all endpoints")
        return None
    except Exception as e:
        print(f"An error occurred while fetching price for security ID {security_id}: {e}")
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




def place_order(dhan, symbol, security_id, lot_size, entry_price, stop_loss, target, trade_type, strategy_key, product_type):
    try:
        log_entry(f"Placing {trade_type.lower()} order for {symbol}: security_id={security_id}, quantity={lot_size}, price={entry_price}, product_type={product_type}")
        log_entry(f"Order Details: Security ID: {security_id}, Entry Price: {entry_price}, Stop Loss: {stop_loss}, Target: {target}")
        transaction_type = BUY if trade_type == 'Long' else SELL
        response = dhan.place_order(
            security_id=str(security_id), 
            exchange_segment=NSE_FNO if product_type == 'FUT' else NSE_EQ,
            transaction_type=transaction_type,
            quantity=lot_size,
            order_type=MARKET,
            product_type=product_type,
            price=0,
            tag=strategy_key
        )
        log_entry(f"Order Response: {response}")
        return response
    except Exception as e:
        log_entry(f"An error occurred while placing order: {e}", "ERROR")
        return None
def process_trade(dhan, symbol, strategy_config):
    try:
        if not within_trading_hours(strategy_config['Start'], strategy_config['Stop']):
            print(f"Outside trading hours for {symbol}")
            return

        sector, industry = get_sector_and_industry(symbol)
        if not check_sector_industry(sector, industry, strategy_config):
            print(f"Sector/Industry mismatch for {symbol}")
            return

        trading_list_df = get_trading_list()
        lots_df = get_lots()

        if trading_list_df is None or lots_df is None:
            print("Failed to fetch trading list or lots data")
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
            print(f"Unsupported instrument type {strategy_config['instrument_type']} for {symbol}. Skipping.")
            return

        if lot_size is None:
            print(f"Failed to determine lot size for {symbol}")
            return

        print(f"Processing {strategy_config['TradeType']} for {symbol_suffix} with product type {strategy_config['product_type']}")
        matches = trading_list_df.loc[trading_list_df['SEM_TRADING_SYMBOL'] == symbol_suffix, 'SEM_SMST_SECURITY_ID']
        if matches.empty:
            print(f"No match found for {symbol_suffix}")
            return
        security_id = matches.values[0]
        
        today = datetime.now().strftime('%Y-%m-%d')
        engine = get_db_connection()
        if engine is not None:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            try:
                query = """
                SELECT SUM(orders_placed) as today_orders_count, 
                       SUM(position_size_placed) as total_position_size_today
                FROM StrategySummary 
                WHERE run_date = %s AND strategy_name = %s
                """
                cursor.execute(query, (today, strategy_config['Strategy']))
                df = cursor.fetchall()
                today_orders_count = df[0][0] or 0
                total_position_size_today = df[0][1] or 0
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

        price_from_json = get_price(security_id)
        
        try:
            ticker = f'{symbol}.NS'
            data = yf.download(ticker, period='1mo', interval='1d')
            if price_from_json:
                entry_price = price_from_json
            else:
                entry_price = round(data['Close'].iloc[-1], 2)
        except Exception as e:
            log_entry(f"Error downloading data from yfinance for {ticker}: {e}", "ERROR")
            return
        
        atr = round(calculate_atr(data), 2)

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

        response = place_order(dhan, symbol_suffix, security_id, lot_size, entry_price, stop_loss, target, strategy_config['TradeType'], strategy_config['Strategy'], strategy_config['product_type'])
        if response and response['status'] == 'success':
            log_entry(f"{strategy_config['TradeType']} order executed for {symbol_suffix}: {response}")
        else:
            log_entry(f"Failed to execute {strategy_config['TradeType']} order for {symbol_suffix}: {response['remarks']}", "ERROR")

    except Exception as e:
        print(f"Error in process_trade for symbol {symbol}: {str(e)}")

def process_alert(alert_data):
    try:
        strategy_name = alert_data['alert_name']
        symbols = alert_data['stocks'].split(',')

        strategy_config = get_strategy_config(strategy_name)
        if strategy_config is None:
            print(f"No strategy configuration found for {strategy_name}")
            return

        dhan = dhanhq(client_id, access_token)

        for symbol in symbols:
            process_trade(dhan, symbol, strategy_config)
    except Exception as e:
        print(f"Error in process_alert: {str(e)}")