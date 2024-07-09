import pandas as pd
import os
import json
import requests
from dhanhq import dhanhq
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
INTRADAY = "INTRADAY"
TICK_SIZE = 0.05  # Tick size for most Indian stocks

# Dhan API client credentials
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')

def get_db_connection():
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        return engine
    except Exception as e:
        logger.error(f"Error while connecting to MySQL: {e}")
        return None

def check_db_connection():
    engine = get_db_connection()
    if engine:
        logger.info("Database connection successful")
        engine.dispose()
    else:
        logger.error("Failed to connect to database")

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
            logger.error(f"Error while fetching trading list: {e}")
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
            logger.error(f"Error while fetching lots: {e}")
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
            logger.error(f"Error while fetching sector and industry for {symbol_ns}: {e}")
            return "Unknown", "Unknown"
        finally:
            if engine is not None:
                engine.dispose()
    else:
        return "Unknown", "Unknown"

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
            logger.info("Strategy summary saved to database successfully.")
        except Exception as e:
            logger.error(f"Error while saving strategy summary to database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
            engine.dispose()
    else:
        logger.error("Failed to connect to the database. Strategy summary not saved.")

def save_trade_log_to_mysql(trade_entries):
    if not trade_entries:
        return
    logger.info(f"Attempting to save {len(trade_entries)} trade entries to MySQL")
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
                        logger.info(f"Duplicate entry found for security_id {entry['security_id']} at {entry['timestamp']}. Skipping insertion.")
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
                    logger.error(f"Error inserting entry into MySQL: {e}")
                    logger.error(f"Problematic entry: {entry}")
            
            connection.commit()
            logger.info("Trade log appended to MySQL database successfully.")
        except Exception as e:
            logger.error(f"Error while saving trade log to database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
            engine.dispose()
    else:
        logger.error("Failed to connect to the database. Trade log not saved.")

def get_price(security_id):
    try:
        url = f'http://139.59.70.202:5000/price/{security_id}'
        logger.info(f"Fetching price from URL: {url}")
        response = requests.get(url, timeout=10)
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response content: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            price = float(data.get('latest_price'))
            logger.info(f"Fetched price for security ID {security_id}: {price}")
            return price
        else:
            logger.error(f"Failed to get price for security ID {security_id}. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception while fetching price for security ID {security_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for security ID {security_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching price for security ID {security_id}: {e}")
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
        logger.info(f"Placing {trade_type.lower()} order for {symbol}: security_id={security_id}, quantity={lot_size}, price={entry_price}, product_type={product_type}")
        logger.info(f"Order Details: Security ID: {security_id}, Entry Price: {entry_price}, Stop Loss: {stop_loss}, Target: {target}")
        transaction_type = BUY if trade_type == 'Long' else SELL
        exchange_segment = NSE_FNO if product_type in ['MARGIN', 'FUT', 'OPT'] else NSE_EQ

        # Log all parameters being sent to the Dhan API
        logger.info(f"Dhan API Parameters: security_id={security_id}, exchange_segment={exchange_segment}, "
                    f"transaction_type={transaction_type}, quantity={lot_size}, order_type={MARKET}, "
                    f"product_type={product_type}, price=0, tag={strategy_key}")

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
        logger.info(f"Full Order Response: {response}")
        return response
    except Exception as e:
        logger.error(f"An error occurred while placing order: {str(e)}")
        return None

def process_trade(dhan, symbol, strategy_config):
    try:
        logger.info(f"Processing trade for {symbol} with strategy {strategy_config['Strategy']}")
        logger.info(f"Instrument type: {strategy_config['instrument_type']}")
        logger.info(f"Product type: {strategy_config['product_type']}")

        if not within_trading_hours(strategy_config['Start'], strategy_config['Stop']):
            logger.info(f"Outside trading hours for {symbol}")
            return

        sector, industry = get_sector_and_industry(symbol)
if not check_sector_industry(sector, industry, strategy_config):
            logger.info(f"Sector/Industry mismatch for {symbol}")
            return

        trading_list_df = get_trading_list()
        lots_df = get_lots()

        if trading_list_df is None or lots_df is None:
            logger.error("Failed to fetch trading list or lots data")
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
            logger.error(f"Unsupported instrument type {strategy_config['instrument_type']} for {symbol}. Skipping.")
            return

        if lot_size is None:
            logger.error(f"Failed to determine lot size for {symbol}")
            return

        logger.info(f"Processing {strategy_config['TradeType']} for {symbol_suffix} with product type {strategy_config['product_type']}")
        matches = trading_list_df.loc[trading_list_df['SEM_TRADING_SYMBOL'] == symbol_suffix, 'SEM_SMST_SECURITY_ID']
        if matches.empty:
            logger.error(f"No match found for {symbol_suffix}")
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

        logger.info(f"Today's orders count for strategy {strategy_config['Strategy']}: {today_orders_count}")
        logger.info(f"Today's total position size for strategy {strategy_config['Strategy']}: {total_position_size_today}")

        if today_orders_count >= strategy_config["Max_Positions"]:
            logger.info(f"Max positions limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_Positions']}. Skipping new order.")
            return

        if total_position_size_today >= strategy_config["Max_PositionSize"]:
            logger.info(f"Max position size limit reached for strategy {strategy_config['Strategy']} on {today}: {strategy_config['Max_PositionSize']}. Skipping new order.")
            return

        price_from_api = get_price(security_id)
        if price_from_api is not None:
            entry_price = price_from_api
            logger.info(f"Using price from API for {symbol_suffix}: {entry_price}")
        else:
            logger.warning(f"Failed to get price from API for {symbol_suffix}. Falling back to yfinance.")
            try:
                ticker = f'{symbol}.NS'
                data = yf.download(ticker, period='1d', interval='1m')
                entry_price = round(data['Close'].iloc[-1], 2)
                logger.info(f"Using price from yfinance for {symbol_suffix}: {entry_price}")
            except Exception as e:
                logger.error(f"Error downloading data from yfinance for {ticker}: {e}")
                return
        
        try:
            ticker = f'{symbol}.NS'
            data = yf.download(ticker, period='1mo', interval='1d')
            atr = round(calculate_atr(data), 2)
        except Exception as e:
            logger.error(f"Error calculating ATR for {ticker}: {e}")
            return

        if atr == 0:
            logger.error(f"ATR calculation resulted in zero, skipping trade for {symbol_suffix}")
            return

        if strategy_config['TradeType'] == 'Long':
            stop_loss = round((entry_price - strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price + strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        elif strategy_config['TradeType'] == 'Short':
            stop_loss = round((entry_price + strategy_config["ATR_SL"] * atr) / TICK_SIZE) * TICK_SIZE
            target = round((entry_price - strategy_config["ATR_Target"] * atr) / TICK_SIZE) * TICK_SIZE
        else:
            logger.error(f"Unknown trade type {strategy_config['TradeType']} for {symbol_suffix}. Skipping.")
            return

        sl_percentage = round(abs((entry_price - stop_loss) / entry_price) * 100, 2)
        target_percentage = round(abs((target - entry_price) / entry_price) * 100, 2)
        max_loss = round(abs((entry_price - stop_loss) * lot_size), 2)
        max_profit = round(abs((target - entry_price) * lot_size), 2)
        position_size = round(entry_price * lot_size, 2)

        logger.info(f"Strategy: {strategy_config['Strategy']}")
        logger.info(f"ATR: {atr}, ATR SL Multiplier: {strategy_config['ATR_SL']}, ATR Target Multiplier: {strategy_config['ATR_Target']}")
        logger.info(f"Entry Price: {entry_price}, Stop Loss: {stop_loss}, Stop Loss %: {sl_percentage}%, Target: {target}, Target %: {target_percentage}%")
        logger.info(f"Max Loss: {max_loss}, Max Profit: {max_profit}")
        logger.info(f"Lot Size: {lot_size}, Position Size: {position_size}")

        response = place_order(dhan, symbol_suffix, security_id, lot_size, entry_price, stop_loss, target, strategy_config['TradeType'], strategy_config['Strategy'], strategy_config['product_type'])
        if response and response['status'] == 'success':
            logger.info(f"{strategy_config['TradeType']} order executed for {symbol_suffix}: {response}")
            
            # Prepare trade entry for logging
            trade_entry = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': symbol_suffix,
                'strategy': strategy_config['Strategy'],
                'action': strategy_config['TradeType'],
                'security_id': security_id,
                'quantity': lot_size,
                'price': entry_price,
                'order_type': MARKET,
                'trigger_price': None,
                'entry_price': entry_price,
                'exit_price': None,
                'stop_loss': stop_loss,
                'target': target,
                'order_status': 'OPEN',
                'response': response,
                'max_profit': max_profit,
                'max_loss': max_loss,
                'trade_type': strategy_config['TradeType'],
                'stop_loss_percentage': sl_percentage,
                'target_percentage': target_percentage,
                'atr_sl_multiplier': strategy_config['ATR_SL'],
                'atr_target_multiplier': strategy_config['ATR_Target'],
                'product_type': strategy_config['product_type'],
                'position_size': position_size,
                'holding_period': None
            }
            save_trade_log_to_mysql([trade_entry])
        else:
            logger.error(f"Failed to execute {strategy_config['TradeType']} order for {symbol_suffix}: {response['remarks']}")

    except Exception as e:
        logger.error(f"Error in process_trade for symbol {symbol}: {str(e)}")

def process_alert(alert_data):
    try:
        strategy_name = alert_data['alert_name']
        symbols = alert_data['stocks'].split(',')

        strategy_config = get_strategy_config(strategy_name)
        if strategy_config is None:
            logger.error(f"No strategy configuration found for {strategy_name}")
            return

        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

        for symbol in symbols:
            process_trade(dhan, symbol, strategy_config)
    except Exception as e:
        logger.error(f"Error in process_alert: {str(e)}")

if __name__ == "__main__":
    check_db_connection()
    # You can add any initialization or test code here
    logger.info("Trading logic script initialized and ready.")