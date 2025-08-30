# Group Project: Movie Recommendations
## Team Jurassic Spark

## Teammates
- Justin Ankrom
- Daniel Hayase
- Hoshi Hu
- Nachiketa Hebbar
- Yukta Bhartia

### Prerequisites

Ensure you have the following installed on your VM:

- Python 3
- `apt` package manager

### Installation Steps

1. **Install Flask:**

   sudo apt install python3-flask
2. **Install curl:**
    sudo apt install curl

3. **Allow incoming requests on port 8082 :**
    sudo iptables -A INPUT -p tcp --dport 8082 -j ACCEPT

4. **navigate to project directory and run the server:** 
         python3 run app.py
5. **test endpont**
    curl http://128.2.205.112:8082/recommend/user_id

## Dan's Notes on his model: 

Should work fine as long as you have the predict file and pkl file in the same directory level. When you call the function, you just need the user_id as a system arg. It currently outputs an array of the movie recommendations but that can easily be tweaked to whatever you need in the code. LMK if you have any questions

## Notes on Nachi's model:

Model name: Alternating Least Squares model. The model parameters are present in nachi_model.pkl file

Training Data: The model was trained on user id, movie ids, and percentage of movie completed(between 0-1). I used the data from kafka_stream.csv to get the percentage of movie watched by users. 

Inference: Model inference can be done using nachi_predict.py. The script contains a load_model_and_recommend_function which takes in the the model_file, user_id and number of recommendations(default=20) and return a list of recommended movie ids. The returned movie list can be sometimes be less than 20 or even Null if the model didnt find any movies to recommend or encounters a user it hasnt seen before.

Model Metrics: I tested the model on 2000 random user ids with the following results: 

               Total total users queried: 2000
               
               Total users processed(with valid recommendations):1166
               
               Total inference time: 3.6 seconds
               
               Avg inference time: 0.003 seconds
               
               Total recommendations returned: 4475, making an average of 3.8 recommendations per valid user
               
               Root Mean Squared Error was around 3 on the testing data(around 20k rows of userids movieid and ratings)


## Notes on Hoshi's models:

Two models using collaborative filtering: 

1) cosine similarity model that uses cosine similarity to find similar users and their ratings
2) matrix factorization model that uses SVD model to break down User vs. Movie matrix

Files needed to start training are: _normalized_implicit_movie_feedback.csv_, _cosine_similarity_train.py_, and _matrix_factorization_train.py_

Files needed for inference: _cosine_similarity_model.pkl_, _matrix_factorization_model.pkl_, _cosine_similarity_predict.py_, and _matrix_factorization_predict.py_
