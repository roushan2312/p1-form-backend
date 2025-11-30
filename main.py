from flask import Flask, request, jsonify
from store_data import store_data_function as run_store_data

app = Flask(__name__)

@app.route('/events', methods=['POST'])

def handle_event():
    try:
        event = request.get_json()

        if not event:
            return jsonify({'error': 'No data received'}), 400

        result= run_store_data(event)
        message= f"Data stored with id: {result['id']}"
        return jsonify({
            'message': message,
            "result": result
        }), 200


    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)