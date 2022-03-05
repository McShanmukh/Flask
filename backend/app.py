from flask import Flask, jsonify, make_response, request
from flask_restful import Resource,Api,reqparse,abort
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import logging
import psycopg2
import json
import re
#establishing the connection
conn = psycopg2.connect(database="economics_schema", user='aidatabases', password='Aidatabases#', host='65.1.96.15', port='5433')
#Setting auto commit false
conn.autocommit = True
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://aidatabases:Aidatabases#@65.1.96.15:5433/economics_schema'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
ma = Marshmallow(app)
api = Api(app)
def adaptStringForPostgres(text):
    txt = text
    print(type(txt))
    if txt and type(txt) == 'string':
        txt = re.sub(r"[^\w\s]", '', txt)
    return txt


def get_data(query):
    cursor.execute(query)
    results = cursor.fetchall()
    row_headers = [x[0]  for x in cursor.description]  # this will extract row headers
    json_data = []
    for result in results :
        json_data.append(dict(zip(row_headers,result)))
    data = jsonify(json_data)
    return (data)    

# @app.route('/hello-world', methods = ['GET'])
class Get_articles(Resource):
    def get(self):
        say_hello = "ippudanna ra ra naayana"
        return say_hello
    
api.add_resource(Get_articles,'/helloCheppranayana')

@app.route('/dashboards', methods = ['GET']) 
def getDashboards():
    query = '''SELECT * FROM economics_v201.dashboards_list'''
    data  = get_data(query)
    var = request
    print(var)
    return make_response(data)


@app.route('/dashimages', methods=['GET'])
def getDashboardImages():
    query = '''SELECT "Thumbnail_Image_link", index FROM economics_v201.dashboards_list'''
    data  = get_data(query)
    return make_response(data)
 

@app.route('/getMainPageCards', methods=['GET'])
def getMainPageCards():
    query = '''SELECT DISTINCT "Master_Table"."Level1", "Master_Table"."Level2" FROM economics_v202."Master_Table" ORDER BY "Master_Table"."Level1"'''
    data  = get_data(query)
    return make_response(data)
 

@app.route('/level2', methods=['POST'])
def level2function():
    query = '''SELECT DISTINCT ON ("Master_Table"."Level2") "Master_Table"."Level2", thumbnail_images."Thumbnail_images_link" FROM economics_v202."Master_Table" FULL JOIN economics_v201.thumbnail_images ON "Master_Table"."Level1" = thumbnail_images."Level1" WHERE "Master_Table"."Level1" = \'${adaptStringForPostgres(level1)}\''''
    data  = get_data(query)
    return make_response(data)
 

@app.route('/rates', methods=['GET'])
def getRates():
    query = '''SELECT * FROM economics_v201.rates'''
    data  = get_data(query)
    return make_response(data)
 
task_post_args = reqparse.RequestParser()
task_post_args.add_argument("level1")
task_post_args.add_argument("level2")

class getLevel3andPath(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2 = args['level1'],args['level2'] 
        # print(args,level1,level2)
        query = 'SELECT DISTINCT ON ("Master_Table"."Level3") "Master_Table"."Level3" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level2" = '+ WadaptStringForPostgres(level2)+' AND "Master_Table"."Level1" = \'${adaptStringForPostgres(level1)}\' ORDER BY "Master_Table"."Level3"'
        print(query)
        data  = get_data(query)
        return data
api.add_resource(getLevel3andPath,'/Level3')

# @app.route('/getLevel4', methods=['GET'])
# def getDashboards():
#     query = "SELECT DISTINCT "Master_Table"."Level4" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level3" = \'${adaptStringForPostgres(level3)}\' AND "Master_Table"."Level2" = \'${adaptStringForPostgres(level2)}\' AND "Master_Table"."Level1" = \'${adaptStringForPostgres(level1)}\' AND "Master_Table"."Frequency" = '${adaptStringForPostgres(frequency)}' AND "Master_Table"."Units" = '${adaptStringForPostgres(unit)}' AND "Master_Table"."Base_Year" = '${adaptStringForPostgres(base)}'"
#     data  = get_data(query)
#     return make_response(data)
 

# @app.route('/getLevelAll3', methods=['GET'])
# def getDashboards():
#     query = '''SELECT * FROM economics_v201.dashboards_list'''
#     data  = get_data(query)
#     return make_response(data)
 
# @app.route('/getDataForLevel3Key', methods=['GET'])
# def getDashboards():
#     query = '''SELECT "Master_Table"."Level3", "Master_Table"."Level4", "Master_Table"."DataSources", "Master_Table"."Notes", "Master_Table"."Description" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" = '${adaptStringForPostgres(level1)}' AND "Master_Table"."Level2" = '${adaptStringForPostgres(level2)}' AND "Master_Table"."Level3" = '${adaptStringForPostgres(level3)}' ORDER BY "Master_Table"."Level3", "Master_Table"."Level4" LIMIT 1'''
#     data  = get_data(query)
#     return make_response(data)
 
# @app.route('/getDashboards', methods = ['GET']) 
# def getDashboards():
#     query = '''SELECT * FROM economics_v201.dashboards_list'''
#     data  = get_data(query)
#     return make_response(data)
 
# @app.route('/getDashboards', methods = ['GET']) 
# def getDashboards():
#     query = '''SELECT * FROM economics_v201.dashboards_list'''
#     data  = get_data(query)
#     return make_response(data)
 

if __name__ == "__main__":
    app.run(debug = True)   