from trading_logic import process_alert

def lambda_handler(event, context):
    try:
        alert_data = event['body']
        process_alert(alert_data)
        return {
            'statusCode': 200,
            'body': 'Alert processed successfully'
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error processing alert: {str(e)}'
        }