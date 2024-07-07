from flask import Flask, request, jsonify
from trading_logic import process_alert

app = Flask(__name__)  # Make sure this line is here and not inside any function

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        alert_data = request.json
        process_alert(alert_data)
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)