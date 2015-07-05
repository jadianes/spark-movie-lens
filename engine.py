from pyspark.mllib.recommendation import MatrixFactorizationModel
 
class RecommendationEngine:
    """
    A movie recommendation engine
    """
    
    def add_ratings(self, user_ratings):
        """
        Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        pass
    
    def get_top_recommendations(self, user_id, movies_count):
        """
        Recommends up to movies_count top unrated movies to user_id
        """
        pass
    
    def __init__(self, spark_context, model_file):
        """
        Init the recommendation engine given a Spark context
        """
        self.sc = spark_context
        # load ALS model from file
        self.model = MatrixFactorizationModel.load(model_file)
 