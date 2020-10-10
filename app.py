from flask import Blueprint,Flask, render_template, flash, redirect, url_for, session, request, logging
from flask_mysqldb import MySQL,MySQLdb
from flask import current_app as app
from engine import RecommendationEngine
from collections import OrderedDict
import ast,json,logging
import urllib.request as ur
from contentbased import content
import time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


main = Blueprint('main', __name__)

@main.route("/" , methods=["GET","POST"])
def home():    
    return render_template('home.html')

@main.route("/logout/<int:user_id>" , methods=["GET","POST"])
def logout(user_id):
    session.clear()    
    return redirect(url_for('main.login'))
    

@main.route("/login" , methods=["GET","POST"])
def login():
    session.clear()
    if request.method == "POST":
        
        userDetails = request.form
        if userDetails['btn'] == "submit":

            ID = userDetails['User_ID']
            Password = userDetails['password_1'].encode('utf-8')
            #hash_password = bcrypt.hashpw(Password, bcrypt.gensalt())
            Name = userDetails['username']
            Value1 = userDetails['value1']
            Value2 = userDetails['value2']
            cur = mysql.connection.cursor()
            cur.execute("INSERT INTO users(ID,Password,Name,Genre1,Genre2) VALUES(%s , %s, %s, %s, %s)", (ID,Password,Name,Value1,Value2))
            mysql.connection.commit()
            session['Name'] = Name
            session['ID'] = ID
            return redirect(url_for('main.new',user_id=ID,Value1=Value1,Value2=Value2))

        if userDetails['btn'] == "submit1":
            
            ID = userDetails['User_ID1']
            Password = userDetails['password'].encode('utf-8')
            curl = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
            curl.execute("SELECT * FROM users WHERE ID =%s",[ID])
            user = curl.fetchone()
            print(user)
            curl.close()
           
            if len(user) > 0:
                if Password == user["Password"].encode('utf-8'):
    
                    session['Name'] = user["Name"]
                    session['ID'] = ID
                    return redirect(url_for('main.index',user_id=user["ID"]))
                else:
                    return "Error password and ID not match"
            else:
                return "Error user not found"
         

    return render_template('login.html')

@main.route("/new/<int:user_id>/<Value1>/<Value2>", methods=["GET"])
def new(user_id,Value1,Value2):
      
    g = [str(Value1),str(Value2)]
    start=time.time()
    t = cb.start(20,g)
    print(time.time()-start)
    t = t.to_json(orient='index')
    data1=ast.literal_eval(t)
    print(data1)
    new_user=1
    return render_template('index.html' ,data1=data1, new_user=new_user, user_id=user_id , active='home',data={})


@main.route("/ratings/<int:user_id>/<int:movie_id>/<int:imd_id>", methods=["GET" , "POST"])
def movie_ratings1(user_id, movie_id, imd_id):
    if request.method == 'GET':
        imd_id = '{:0>7}'.format(imd_id)
        logger.debug("User %s rating requested for movie %s", user_id, movie_id)
        url = " http://www.omdbapi.com/?i=tt"+imd_id+"&apikey=f36552d0"
        response = ur.urlopen(url)
        u = json.loads(response.read())
        x = (float(u['imdbRating'])/10)*100
        rate = float(u['imdbRating'])/2

        flag = 0
        return render_template('single.html', u=u,user_id=user_id,movie_id=movie_id,flag=flag,x=x,rate=rate) 
    
    if request.method == 'POST':
        userDetails = request.form
        ratings_list = userDetails['rating']
        ratings1 = [[user_id,movie_id,float(ratings_list)]]
        

        recommendation_engine.add_ratings(ratings1)
        return redirect(url_for('main.index',user_id=user_id))     

@main.route("/index/<int:user_id>", methods=["GET"])
def index(user_id):
    logger.debug("User %s rated movies requested", user_id)
    ratings = recommendation_engine.get_user_ratings(user_id)
    # print(ratings)
    # print(type(ratings))
    if ratings.empty:
        curl = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        curl.execute("SELECT * FROM users WHERE ID =%s",[user_id])
        user = curl.fetchone()
        Value1 = user["Genre1"]
        Value2 = user["Genre2"] 
        curl.close()
        print(Value1)
        return redirect(url_for('main.new',user_id=user_id,Value1=Value1,Value2=Value2)) 
    
    t = ratings.to_json(orient='index')
    data=ast.literal_eval(t)
    data = OrderedDict(sorted(data.items(), key= lambda x: x[1]['2'], reverse=True))
    return render_template('index.html' ,data=data ,user_id=user_id , active='home' , data1 ={})
    

@main.route("/ratings/top/<int:user_id>/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    t = top_ratings.to_json(orient='index')
    data=ast.literal_eval(t)
    data = OrderedDict(sorted(data.items(), key= lambda x: x[1]['2'], reverse=True))
    return render_template('review.html', data=data ,user_id=user_id , active='review') 


@main.route("/exturl/<int:imd_id>", methods=["GET"])
def exturl(imd_id):
    imd_id = '{:0>7}'.format(imd_id)
    url = " http://www.omdbapi.com/?i=tt"+imd_id+"&apikey=f36552d0"
    response = ur.urlopen(url)
    ux = json.loads(response.read())
    print("Poster url"+ux['Poster'])
    return redirect(ux['Poster'])

@main.route("/exturl1/<int:imd_id>", methods=["GET"])
def exturl1(imd_id):
    imd_id = '{:0>7}'.format(imd_id)
    url = " http://www.omdbapi.com/?i=tt"+imd_id+"&apikey=f36552d0"
    response = ur.urlopen(url)
    ux = json.loads(response.read())
    return ux


@main.route("/ratings/<int:user_id>/<int:movie_id>/<int:imd_id>/<float:rate>", methods=["GET" , "POST"])
def movie_ratings(user_id, movie_id, imd_id, rate):
    if request.method == 'GET':
        logger.debug("User %s rating requested for movie %s", user_id, movie_id)
        ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
        if ratings.empty:
            return redirect(url_for('main.movie_ratings1',user_id=user_id, movie_id=movie_id, imd_id=imd_id))

        t = ratings.to_json(orient='index')
        data=ast.literal_eval(t)
        data = OrderedDict(sorted(data.items(), key= lambda x: x[1]['2'], reverse=True))
        url = " http://www.omdbapi.com/?i=tt"+data['0']['4']+"&apikey=f36552d0"
        response = ur.urlopen(url)
        u = json.loads(response.read())
        x = (data['0']['2']/5)*100
        
        ratings2 = recommendation_engine.get_user_ratings(user_id)
        t1 = ratings2.to_json(orient='index')
        datax=ast.literal_eval(t1)
        flag = 0
        for p_id, p_info in datax.items():
            if p_info['0'] == movie_id:
               flag = 1

        return render_template('single1.html', data=data , u=u , x=x ,user_id=user_id,movie_id=movie_id,flag=flag) 
    
    if request.method == 'POST':
        userDetails = request.form
        ratings_list = userDetails['rating']
        ratings1 = [[user_id,movie_id,float(ratings_list)]]
        

        recommendation_engine.add_ratings(ratings1)
        return redirect(url_for('main.index',user_id=user_id)) 

 
@main.route("/ratings/<int:user_id>/<int:movie_id>/<name>", methods=["GET" , "POST"])
def item_ratings(user_id, movie_id, name):
    logger.debug("Movie similar to %s requested", movie_id)
    top_ratings1 = recommendation_engine.get_recommend_for_movie_id(movie_id)
    t1 = top_ratings1.to_json(orient='index')
    data1=ast.literal_eval(t1)
    data1 = OrderedDict(sorted(data1.items(), key= lambda x: x[1]['4'], reverse=True))
    return render_template('item.html',data1=data1,user_id=user_id,movie_id=movie_id,name=name)



 
def create_app(spark_context, dataset_path):
    global recommendation_engine 
    global cb
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    cb = content(dataset_path)
    app = Flask(__name__)
    app.secret_key = "^A%DJAJU^JJ123"
    app.config['MYSQL_HOST'] = 'localhost'
    app.config['MYSQL_USER'] = 'root'
    app.config['MYSQL_PASSWORD'] = ''
    app.config['MYSQL_DB']= 'flaskapp'
    app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
    global mysql
    mysql=MySQL(app)
    app.register_blueprint(main)
    return app 
