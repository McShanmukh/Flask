from flask import Flask, jsonify, make_response, request
from flask_restful import Resource,Api,reqparse,abort
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import logging
import psycopg2
import json
import re
#establishing the connection
conn = psycopg2.connect(database="demographics_gis", user='aidatabases',
                        password='Aidatabases#', host='65.1.96.15', port='5433')
#Setting auto commit false
conn.autocommit = True
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://aidatabases:Aidatabases#@65.1.96.15:5433/demographics_gis'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
ma = Marshmallow(app)
api = Api(app)
def adaptStringForPostgres(text):
    txt = text
    if txt and type(txt) == 'string':
        txt = re.sub(r"[^\w\s]", '', txt)
    return txt
## Getting data from Postman input
task_post_args = reqparse.RequestParser()
task_post_args.add_argument("level1")
task_post_args.add_argument("level2")
task_post_args.add_argument("level3")
task_post_args.add_argument("level4")
task_post_args.add_argument("frequency")
task_post_args.add_argument("unit")
task_post_args.add_argument("base")
task_post_args.add_argument("name")
task_post_args.add_argument("email")
task_post_args.add_argument("dataset")
task_post_args.add_argument("purpose")
task_post_args.add_argument("source")
task_post_args.add_argument("keyword")
task_post_args.add_argument("level")

## fn for running query in Postgresql
def get_data(query):
    cursor.execute(query)
    results = cursor.fetchall()
    row_headers = [x[0]  for x in cursor.description]  # this will extract row headers
    json_data = []
    for result in results :
        json_data.append(dict(zip(row_headers,result)))
    data = jsonify(json_data)
    return (data)    

class getVariables(Resource):
    def get(self):
        query = '''SELECT DISTINCT mgnrega_geojson."Variable" FROM mgnrega_schema.mgnrega_geojson ORDER BY mgnrega_geojson."Variable"'''
        data  = get_data(query)
        var = request
        print(var)
        return make_response(data)


class getAllLevels(Resource):
    def get(self):
        query = '''SELECT DISTINCT mgnrega_geojson."Level 1", mgnrega_geojson."Level 2", mgnrega_geojson."Level 3" FROM mgnrega_schema.mgnrega_geojson ORDER BY mgnrega_geojson."Level 1"'''
        data  = get_data(query)
        return make_response(data)
class getAllStates(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1,level2,level3= args['level1'],args['level2'],args['level3']
        query = """SELECT DISTINCT mgnrega_geojson."State_Name" FROM mgnrega_schema.mgnrega_geojson WHERE mgnrega_geojson."Level 1" = '"""+level1+"""' AND mgnrega_geojson."Level 2" = '"""+level2+"""' AND mgnrega_geojson."Level 3" = '"""+level3+"""' ORDER BY mgnrega_geojson."State_Name" """
        data  = get_data(query)
        return make_response(data)


        
api.add_resource(getVariables, '/Level1')
api.add_resource(getAllLevels, '/levels')
api.add_resource(getAllStates, '/states')


if __name__ == "__main__":
    app.run(debug = True)   