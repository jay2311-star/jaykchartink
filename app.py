from flask import Flask, request, jsonify
from trading_logic import process_alert
import pymysql
import os

app = Flask(__name__)

# Database connection details
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        alert_data = request.json
        process_alert(alert_data)
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/dhan-webhook', methods=['POST'])
def dhan_webhook():
    data = request.json
    
    # Insert data into the database
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO dhan_order_updates (
                dhan_client_id, order_id, correlation_id, order_status, transaction_type,
                exchange_segment, product_type, order_type, validity, trading_symbol,
                security_id, quantity, disclosed_quantity, price, trigger_price,
                after_market_order, bo_profit_value, bo_stop_loss_value, leg_name,
                create_time, update_time, exchange_time, drv_expiry_date,
                drv_option_type, drv_strike_price, oms_error_code, oms_error_description
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.execute(sql, (
                data.get('dhanClientId'), data.get('orderId'), data.get('correlationId'),
                data.get('orderStatus'), data.get('transactionType'), data.get('exchangeSegment'),
                data.get('productType'), data.get('orderType'), data.get('validity'),
                data.get('tradingSymbol'), data.get('securityId'), data.get('quantity'),
                data.get('disclosedQuantity'), data.get('price'), data.get('triggerPrice'),
                data.get('afterMarketOrder'), data.get('boProfitValue'), data.get('boStopLossValue'),
                data.get('legName'), data.get('createTime'), data.get('updateTime'),
                data.get('exchangeTime'), data.get('drvExpiryDate'), data.get('drvOptionType'),
                data.get('drvStrikePrice'), data.get('omsErrorCode'), data.get('omsErrorDescription')
            ))
        conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()
    
    return jsonify({'status': 'success'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)