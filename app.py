from flask import Flask , jsonify
from nachi_model_predict import load_model_and_recommend 
import requests

app = Flask(__name__)

KAFKA_IP="128.2.204.215"

@app.route('/recommend/<int:user_id>', methods=['GET'])
def recommend_movies(user_id):
    try:
        # data quality check
        user_info_req = requests.get(f"http://{KAFKA_IP}:8080/user/{user_id}")
        if user_info_req.status_code != 200:
            return jsonify({'error': 'User ID not found.'}), 404
        
        recommended_movies = load_model_and_recommend('nachi_model.pkl', user_id, n_recommendations=20)
        if not recommended_movies:
            return jsonify({'error': 'No recommendations found or user ID not found.'}), 404

        
        return ",".join(recommended_movies), 200
        

    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='128.2.205.112', port=8082)