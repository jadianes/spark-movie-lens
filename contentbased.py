import os
import csv
import numpy as np
import pandas as pd
from ast import literal_eval
import collections
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.feature_extraction.text import CountVectorizer

from sklearn.metrics.pairwise import linear_kernel
from sklearn.metrics.pairwise import cosine_similarity


class content:



         

    def get_recommendations(self,k, cosine_sim):

        # Get the index of the movie that matches the title
        idx = 0

        # Get the pairwsie similarity scores of all movies with that movie
        sim_scores = list(enumerate(cosine_sim[idx]))

        # Sort the movies based on the similarity scores
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

        # Get the scores of the 10 most similar movies
        sim_scores = sim_scores[1:k]

        # Get the movie indices
        movie_indices = [i[0] for i in sim_scores]

        # Return the top 10 most similar movies
        return movies['title'].iloc[movie_indices]

    def get_list(self,x):

        # if isinstance(x, String):
        names = [i[:] for i in x]
        #Check if more than 3 elements exist. If yes, return only first three. If no, return entire list.
        # if len(names) > 10:
        #     names = names[:10]
        return names

        #Return empty list in case of missing/malformed data
        # return []

    def clean_data(self,x):

        if isinstance(x, list):
            return [str.lower(i.replace(" ", "")) for i in x]
        else:
            #Check if director exists. If not, return empty string
            if isinstance(x, str):
                return str.lower(x.replace(" ", ""))
            else:
                return ''

    def create_soup(self,x):

        return ' '.join(x['genres'])



    def start(self,k,g):
        features_file_path = os.path.join(self.dataset_path,'movies.csv')
        features_file_path1 = os.path.join(self.dataset_path,'links.csv')
      
        global movies

        movies = pd.read_csv(features_file_path,low_memory=True)
        movies1 = pd.read_csv(features_file_path,low_memory=True)
        links = pd.read_csv(features_file_path1,low_memory=True)
        # type(movies)
        movies['genres'].replace('\|', ' ',regex=True,inplace=True)
        movies['genres'].head()
        # len(movies)




        # tfidf = CountVectorizer(stop_words="english")
        # #Replace NaN with an empty string
        # movies['genres'] = movies['genres'].fillna('')

        # #Construct the required TF-IDF matrix by fitting and transforming the data
        # tfidf_matrix = tfidf.fit_transform(movies['genres'])

        # #Output the shape of tfidf_matrix
        # tfidf_matrix.shape
        # print(tfidf_matrix)
        # cosine_sim = linear_kernel(tfidf_matrix[0:][0:], tfidf_matrix[0:1000][0:])
        # print(cosine_sim.shape)
        # print(cosine_sim)
        # indices = pd.Series(movies.index, index=movies['title']).drop_duplicates()
        # indices[0]
        # get_recommendations("Toy Story (1995)")

        movies['movieId'] = movies['movieId'].astype('int')
        movies['genres'] = movies['genres'].str.split(" ")
        # movies['genres'] = movies['genres'].apply(literal_eval)
        movies['genres'] = movies['genres'].apply(self.get_list)
        type(movies['genres'])
        movies['genres'] = movies['genres'].apply(self.clean_data)
        movies.head()


        movies['soup'] = movies.apply(self.create_soup, axis=1)

    
        col_names =  ['movieId', 'title', 'genres']
        my_df  = pd.DataFrame(columns = col_names)
        my_df.loc[len(my_df)] = [0, 'user pref', g]
        print(my_df)
        my_df['genres'] = my_df['genres'].apply(self.clean_data)
        my_df['soup'] = my_df.apply(self.create_soup, axis=1)




        movies['soup'].head()
        # type(genre)
        # type(movies['soup'])
        count = CountVectorizer(stop_words='english')
        count_matrix = count.fit_transform(movies['soup'])
        temp = zip(count.get_feature_names(),
            np.asarray(count_matrix.sum(axis=0)).ravel())
        print(temp)
        x = { k:0 for k, v in temp}
        x= collections.OrderedDict(sorted(x.items()))
        print(x)
        for i in g:
            x[i] = 1

        print(x)

        x = [value for key, value in x.items()]
        print(x)

        # count_genre = count.fit_transform(x)
        # # print(count_matrix)
        # print(count_genre)
        x = np.array(x)
        x = x.reshape(-1,23)
        print(x.shape)
        cosine_sim2 = linear_kernel(x,count_matrix)
        print(cosine_sim2)
        # movies = movies.reset_index()
        indices = pd.Series(movies.index, index=movies['title'])
        
        t = self.get_recommendations(k, cosine_sim=cosine_sim2)
        print(t)
        col_names1 =  ['title']
        t  = pd.DataFrame(t,columns = col_names1)
        print(t)
        
        # t['movieId'] = t['movieId'].astype('int')
        links['movieId'] = links['movieId'].astype('int')
        t = t.merge(movies1,on = "title")

        t = t.merge(links,on = "movieId")
        print(t)
        return t


    def __init__(self , dataset_path):
        self.dataset_path = dataset_path
        print("content based initialized")

