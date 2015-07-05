
# An on-line movie recommending service using Spark & Flask - Building the web service  

This tutorial goes into detail into how to use Spark machine learning models, or even other kind of data anlytics objects, within a web service. This open the door to on-line predictions, recommendations, etc. By using the Python language, we make this task very easy, thanks to Spark own Python capabilities, and to Python-based frameworks such as Flask. 

This tutorial can be used independently, to build web-services on top of any kind of Spark models. However, it combines powerfully with our tutorial on using Spark MLlib to build a movie recommender model based on the MovieLens dataset. By doing so, you will be able to develop a complete **on-line movie recommendation service**.  

Our complete web service contains three Python files:  
    
- `engine.py` defines the recommendation engine, wrapping insde all the Spark related computations.  
- `app.py` is a Flask web application that defines a RESTful-like API around the engine.  
- `server.py` initialises a *CherryPy* webserver after creating a Spark context and Flask web app using the previous.  

But let's explain each of them in detail, together with the pecualiraties of deploying such a system using Spark as a computation engine. We will put the emphasis on how to use a Spark model within the web context we are dealing with. For an explanation on the MovieLens data and how to build the model using Spark, have a look at the tutorial about Building the Model.  

## A recommendation engine

At the very core of our movie recommendation web service resides a recommendation engine (i.e. `engine.py` in our final deployment). It is represented by the class `RecommendationEngine` and this section will describe step by step how its functionality and implementation.  

### Starting the engine

When the engine is initialised, we need to geenrate the ALS model for the first time. Optionally (we won't do it here) we might be loading a previously persisted model in order to use it for recommendations. Moreover, we might need to load or precompute any RDDs that will be used later on to make recommendations.      

We will do things of that kind in the `__init__` method of our `RecommendationEngine` class (making use of two private methods). In this case, we won't save any time. We will repeat the whole process every time the engine is creates.    


    import os
    from pyspark.mllib.recommendation import ALS
     
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    
    
    class RecommendationEngine:
        """A movie recommendation engine
        """
     
        def __count_and_average_ratings(self):
            """Updates the movies ratings counts from 
            the current data self.ratings_RDD
            """
            logger.info("Counting movie ratings...")
            movie_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
            movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
            self.movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))
     
     
        def __train_model(self):
            """Train the ALS model with the current dataset
            """
            logger.info("Training the ALS model...")
            self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                                   iterations=self.iterations, lambda_=self.regularization_parameter)
            logger.info("ALS model built!")
     
     
        def __init__(self, sc, dataset_path):
            """Init the recommendation engine given a Spark context and a dataset path
            """
     
            logger.info("Starting up the Recommendation Engine: ")
     
            self.sc = sc
     
            # Load ratings data for later use
            logger.info("Loading Ratings data...")
            ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
            ratings_raw_RDD = self.sc.textFile(ratings_file_path)
            ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
            self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
                .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
            # Load movies data for later use
            logger.info("Loading Movies data...")
            movies_file_path = os.path.join(dataset_path, 'movies.csv')
            movies_raw_RDD = self.sc.textFile(movies_file_path)
            movies_raw_data_header = movies_raw_RDD.take(1)[0]
            self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_raw_data_header)\
                .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
            self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
            # Pre-calculate movies ratings counts
            self.__count_and_average_ratings()
     
            # Train the model
            self.rank = 8
            self.seed = 5L
            self.iterations = 10
            self.regularization_parameter = 0.1
            self.__train_model() 

All the code form the `__init__` and the two private methods has been explained in the tutorial about Building the Model.   

### Adding new ratings

When using *Collaborative Filtering* and Spark's *Alternating Least Squares*, we need to recompute the prediction model for every new batch of user ratings. This was explained in our previous tutorial on building the model.  


    def add_ratings(self, ratings):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute movie ratings count
        self.__count_and_average_ratings()
        # Re-train the ALS model with the new ratings
        self.__train_model()
    
        return ratings
    
    # Attach the function to a class method
    RecommendationEngine.add_ratings = add_ratings

### Making recommendations

We also explained how to make recommendations with our ALS model in the tutorial about building the movie recommender. Here, we will basically repeat equivalent code, wrapped inside a method of our `RecommendationEnginer` class, and making use of a private method that will be used for every predicting method.


    def __predict_ratings(self, user_and_movie_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(self.movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
    
        return predicted_rating_title_and_count_RDD
        
    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_RDD = self.movies_RDD.filter(lambda rating: not rating[1]==user_id).map(lambda x: (user_id, x[0]))
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_movies_RDD).filter(lambda r: r[2]>=25).takeOrdered(movies_count, key=lambda x: -x[1])
    
        return ratings
    
    # Attach the functions to class methods
    RecommendationEngine.__predict_ratings = __predict_ratings
    RecommendationEngine.get_top_ratings = get_top_ratings

Apart form getting the top unrated movies, we will also want to get ratings to particular movies. We will do so with a new mothod in our `RecommendationEngine`.  


    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_movies_RDD).collect()
    
        return ratings
    
    # Attach the function to a class method
    RecommendationEngine.get_ratings_for_movie_ids = get_ratings_for_movie_ids

## Building a web API around our engine using Flask

[Flask](http://flask.pocoo.org/) is a web microframework for Python. It is very easy to start up a web API, by just importing in in our script and using some annotations to associate our service end-points with Python functions. In our case we will wrap our `RecommendationEngine` methods around some of these end-points and interchange `JSON` formatted data with the web client.  

In fact is so simple that we will show the whole `app.py` here, instead of going piece by piece.


    from flask import Blueprint
    main = Blueprint('main', __name__)
     
    import json
    from engine import RecommendationEngine
     
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
     
    from flask import Flask, request
     
    @main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
    def top_ratings(user_id, count):
        logger.debug("User %s TOP ratings requested", user_id)
        top_ratings = recommendation_engine.get_top_ratings(user_id,count)
        return json.dumps(top_ratings)
     
    @main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
    def movie_ratings(user_id, movie_id):
        logger.debug("User %s rating requested for movie %s", user_id, movie_id)
        ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
        return json.dumps(ratings)
     
     
    @main.route("/<int:user_id>/ratings", methods = ["POST"])
    def add_ratings(user_id):
        # get the ratings from the Flask POST request object
        ratings_list = request.form.keys()[0].strip().split("\n")
        ratings_list = map(lambda x: x.split(","), ratings_list)
        # create a list with the format required by the negine (user_id, movie_id, rating)
        ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
        # add them to the model using then engine API
        recommendation_engine.add_ratings(ratings)
     
        return json.dumps(ratings)
     
     
    def create_app(spark_context, dataset_path):
        global recommendation_engine 
     
        recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
        
        app = Flask(__name__)
        app.register_blueprint(main)
        return app

Basically we use the app as follows:  

- We init the thing when calling `create_app`. Here the `RecommendationEngine` object is created and then we associate the `@main.route` annotations defined above. Each annotation is defined by (see [Flask docs](http://flask.pocoo.org/docs/0.10/)):  
 - A route, that is its URL and may contain parameters between <>. They are mapped to the function arguments.  
 - A list of HTTP available methods.  
- There are three of these annotations defined, that correspond with the three `RecommendationEngine` methods:  
  - `GET /<user_id>/ratings/top` get top recommendations from the engine.  
  - `GET /<user_id>/ratings` get predicted rating for a individual movie.  
  - `POST /<user_id>/ratings` add new ratings. The format is a series of lines (ending with the newline separator) with `movie_id` and `rating` separated by commas. For example, the following file corresponds to the ten new user ratings used as a example in the tutorial about building the model:    
 

`260,9  
1,8  
16,7  
25,8  
32,9  
335,4  
379,3  
296,7  
858,10  
50,8`  

## Deploying a WSGI server using CherryPy

Among other things, the [CherryPy framework](http://www.cherrypy.org/) features a reliable, HTTP/1.1-compliant, WSGI thread-pooled webserver. It is also easy to run multiple HTTP servers (e.g. on multiple ports) at once. All this makes it a perfect candidate for an easy to deploy production web server for our on-line recommendation service.  

The use that we will make of the CherryPy server is relatively simple. Again we will show here the complete `server.py` script and then explain it a bit.  


    import time, sys, cherrypy, os
    from paste.translogger import TransLogger
    from app import create_app
    from pyspark import SparkContext, SparkConf
     
    def init_spark_context():
        # load spark context
        conf = SparkConf().setAppName("movie_recommendation-server")
        # IMPORTANT: pass aditional Python modules to each worker
        sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
     
        return sc
     
     
    def run_server(app):
     
        # Enable WSGI access logging via Paste
        app_logged = TransLogger(app)
     
        # Mount the WSGI callable object (app) on the root directory
        cherrypy.tree.graft(app_logged, '/')
     
        # Set the configuration of the web server
        cherrypy.config.update({
            'engine.autoreload.on': True,
            'log.screen': True,
            'server.socket_port': 5432,
            'server.socket_host': '0.0.0.0'
        })
     
        # Start the CherryPy WSGI web server
        cherrypy.engine.start()
        cherrypy.engine.block()
     
     
    if __name__ == "__main__":
        # Init spark context and load libraries
        sc = init_spark_context()
        dataset_path = os.path.join('datasets', 'ml-latest')
        app = create_app(sc, dataset_path)
     
        # start web server
        run_server(app)

This is pretty standard use of `CherryPy`. If we have a look at the `__main__` entry point, we do three things:  

- Create a spark context as defined in the function `init_spark_context`, passing aditional Python modules there.  
- Create the Flask app calling the `create_app` we defined in `app.py`.  
- Run the server itself.  


See the following section about starting the server.  

## Running the server with Spark

In order to have the server running while being able to access a Spark context and cluster, we need to submit the `server.py` file to `pySpark` by using `spark-submit`. The different parameters when using this command are better explained in the [Spark](https://spark.apache.org/docs/latest/submitting-applications.html) docummentaion. In our case, we will use something like the following.
~/spark-1.3.1-bin-hadoop2.6/bin/spark-submit --master spark://169.254.206.2:7077 --total-executor-cores 14 --executor-memory 6g server.py 
The important bits are:  

- Use `spark-submit` and not `pyspark` directly.  
- The `--master` parameters must point to your Spark cluster setup (can be local).  
- You can pass additional configuration parameters such as `--total-executor-cores` and `--executor-memory`  

You will see an output like the following:
INFO:engine:Starting up the Recommendation Engine: 
INFO:engine:Loading Ratings data...
INFO:engine:Loading Movies data...
INFO:engine:Counting movie ratings...
INFO:engine:Training the ALS model...
       ... More Spark and CherryPy logging
INFO:engine:ALS model built!                                                                                                 
[05/Jul/2015:14:06:29] ENGINE Bus STARTING
[05/Jul/2015:14:06:29] ENGINE Started monitor thread 'Autoreloader'.
[05/Jul/2015:14:06:29] ENGINE Started monitor thread '_TimeoutMonitor'.
[05/Jul/2015:14:06:29] ENGINE Serving on http://0.0.0.0:5432
[05/Jul/2015:14:06:29] ENGINE Bus STARTED
### Some considerations when using multiple Scripts and Spark-submit

There are two issues we need to work around when using Spark in a deployment like this. The first one is that a Spark cluster is a distrubuted environment of **Workers** orchestrated from the Spark **Master** where the Python script is launched. This means that the master is the only one with access to the submitted script and local additional files. If we want the workers to be able to access additional imported Python moules, they either have to be part of our Python distributuon or we need to pass them implicitly. We do this by using the `pyFiles=['engine.py', 'app.py']` parameter when creating the `SparkContext` object. 

The second issue is related with the previous one but is a bit more tricky. In Spark, when using transformations (e.g. `map` on an RDD), we cannot make reference to other RDDs or objects that are not globally available in the execution context. For example, we cannot make reference to a class instance variables. Because of this, we have defined all the functions that are passed to RDD transformations outside the `RecommendationEgine` class.  

## Trying the service

Let's now give the service a try, using the same data we used on the tutorial about building the model. That is, first we are going to add ratings, and then we are going to get top ratings and individual ratings.

### POSTing new ratings

So first things first, we need to have our service runing as explained in the previous section. Once is running, we will use `curl` to post new ratings from the shell. If we have the file `user_ratings.file` (see **Getting the source code** below) in the current folder, just execute the following command.  
curl --data-binary @user_ratings.file http://<SERVER_IP>:5432/0/ratings
Replacing `<SERVER_IP>` with the IP address where you have the server running (e.g. `localhost` or `127.0.0.1` if its running locally). This command will start some computations and end up with an output representing the ratings that has been submitted as a list of lists (this is just to check that the process was successfull).

In the server output window you will see the actual Spark computation output together with CherryPy's output messages about HTTP requests.  

### GETing top recommendations

This one you can do it using also curl, but the JSON rendering will be better if you just use your web browser. Just go to `http://<SERVER_IP>:5432/0/ratings/top/10` to get the top 10 movie recommendations for the user we POSTed recommendations for.  The results should match with those seen in the tutorial about Building the Model.  

### GETing individual ratings

Similarly, we can curl/navigate to `http://<SERVER_IP>:5432/0/ratings/500 ` to get the predicted rating for the movie *The Quiz (1994)*.

## Getting the source code

The source code for the three Python files, together with additional files, that compose our web service can be found in the [following GISTs](https://gist.github.com/jadianes/85c31c72dc96b036372e).
