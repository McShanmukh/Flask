from flask import Flask, jsonify, make_response, request
from flask_restful import Resource,Api,reqparse,abort
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import logging
import psycopg2
import json
import re
#establishing the connection
conn = psycopg2.connect(database="economics_schema", user='aidatabases', password='Aidatabases#', host='postgres.aidatabases.in', port='5432')
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

class getDashboards(Resource):
    def get(self):
        query = '''SELECT * FROM economics_v201.dashboards_list'''
        data  = get_data(query)
        var = request
        print(var)
        return make_response(data)

class getDashboardImages(Resource):
    def get(self):
        query = '''SELECT "Thumbnail_Image_link", index FROM economics_v201.dashboards_list'''
        data  = get_data(query)
        return make_response(data)

class getMainPageCards(Resource):
    def get(self):
        query = '''SELECT DISTINCT "Master_Table"."Level1", "Master_Table"."Level2" FROM economics_v202."Master_Table" ORDER BY "Master_Table"."Level1"'''
        data  = get_data(query)
        return make_response(data)
 

class level2function(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1 = args['level1']
        query = """SELECT DISTINCT ON ("Master_Table"."Level2") "Master_Table"."Level2", thumbnail_images."Thumbnail_images_link" FROM economics_v202."Master_Table" FULL JOIN economics_v201.thumbnail_images ON "Master_Table"."Level1" = thumbnail_images."Level1" WHERE "Master_Table"."Level1" = '"""+adaptStringForPostgres(level1)+"'"
        data  = get_data(query)
        return make_response(data)
 
class getRates(Resource):
    def get(self):
        query = '''SELECT * FROM economics_v201.rates'''
        data  = get_data(query)
        return make_response(data)
 


class getLevel3andPath(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2 = args['level1'],args['level2'] 
        query = """SELECT DISTINCT ON ("Master_Table"."Level3") "Master_Table"."Level3" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level2" = '"""+ adaptStringForPostgres(level2)+"""' AND "Master_Table"."Level1" = '"""+adaptStringForPostgres(level1)+"""' ORDER BY "Master_Table"."Level3" """
        # print(query)        
        data  = get_data(query)
        return data

class getLevel4(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2, level3, frequency, unit, base = args['level1'], args['level2'], args['level3'], args['frequency'], args['unit'], args['base']
        query = """SELECT DISTINCT "Master_Table"."Level4" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level3" = '"""+adaptStringForPostgres(level3)+"""' AND "Master_Table"."Level2" = '"""+adaptStringForPostgres(level2)+"""' AND "Master_Table"."Level1" = '"""+adaptStringForPostgres(level1)+"""' AND "Master_Table"."Frequency" = '"""+adaptStringForPostgres(frequency)+"""' AND "Master_Table"."Units" = '"""+adaptStringForPostgres(unit)+"""' AND "Master_Table"."Base_Year" = '"""+adaptStringForPostgres(base)+"'"
        # print(query)
        data  = get_data(query)
        return data

class getLevelAll3(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2 = args['level1'],args['level2'] 
        print(args)
        query = """SELECT "Master_Table"."Level3", "Master_Table"."Level4", "Master_Table"."DataSources", "Master_Table"."Notes", "Master_Table"."Units","Master_Table"."Base_Year", "Master_Table"."Description" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" = '""" + adaptStringForPostgres(level1)+ """' AND "Master_Table"."Level2" = '"""+adaptStringForPostgres(level2)+"""' ORDER BY "Master_Table"."Level3", "Master_Table"."Level4" LIMIT 1"""
        # print(query)
        data  = get_data(query)
        return data

class getDataForLevel3Key(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2, level3 = args['level1'], args['level2'], args['level3']
        query = """SELECT "Master_Table"."Level3", "Master_Table"."Level4", "Master_Table"."DataSources", "Master_Table"."Notes", "Master_Table"."Description" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" = '"""+adaptStringForPostgres(level1)+"""' AND "Master_Table"."Level2" = '"""+adaptStringForPostgres(level2)+"""' AND "Master_Table"."Level3" = '"""+adaptStringForPostgres(level3)+"""' ORDER BY "Master_Table"."Level3", "Master_Table"."Level4" LIMIT 1"""
        data  = get_data(query)
        return data


class getTableData(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2, level3, frequency, unit, base = args['level1'], args['level2'], args['level3'], args['frequency'], args['unit'], args['base']
        query = """SELECT * FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" = '"""+adaptStringForPostgres(level1)+"""' AND "Master_Table"."Level2" = '"""+adaptStringForPostgres(level2)+"""' AND "Master_Table"."Level3" = '"""+adaptStringForPostgres(level3)+"""' AND "Master_Table"."Frequency" = '"""+adaptStringForPostgres(frequency)+"""' AND "Master_Table"."Units" = '"""+adaptStringForPostgres(unit)+"""' AND "Master_Table"."Base_Year" = '"""+adaptStringForPostgres(base)+"""' ORDER BY "Master_Table"."Period", "Master_Table"."Level4" """
        data  = get_data(query)
        return data

class getChartsData(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2, level3, level4 = args['level1'], args['level2'], args['level3'], args['level4']
        query = """SELECT * FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" = '"""+adaptStringForPostgres(level1)+"""' AND "Master_Table"."Level2" = '"""+adaptStringForPostgres(level2)+"""' AND "Master_Table"."Level3" = '"""+adaptStringForPostgres(level3)+"""' AND "Master_Table"."Level4" = '"""+adaptStringForPostgres(level4)+"""' ORDER BY "Master_Table"."Period" """
        data  = get_data(query)
        return data

class getLevel3Filters(Resource):
    def post(self):
        args = task_post_args.parse_args()
        level1, level2, level3 = args['level1'], args['level2'], args['level3']
        query = """ SELECT DISTINCT "Master_Table"."Units", "Master_Table"."Base_Year", "Master_Table"."Frequency" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" = '"""+adaptStringForPostgres(level1)+"""' AND "Master_Table"."Level2" = '"""+adaptStringForPostgres(level2)+"""' AND "Master_Table"."Level3" = '"""+adaptStringForPostgres(level3)+"'"
        data  = get_data(query)
        return data


class suggestDataset(Resource):
    def post(self):
        args = task_post_args.parse_args()
        user_name, email, dataset, purpose, source = args['name'], args['email'], args['dataset'], args['purpose'], args['source']
        query = """INSERT INTO suggested_datasets (username, email, dataset, purpose, datasource, verified_mail) VALUES ('"""+user_name + \
            """', '"""+email+"""', '"""+dataset+"""', '"""+purpose + \
                """', '"""+source+"""', 'mohanmcsn8@gmail.com') """   # RETURNING "username" for checking data OP
        data  = get_data(query)
        return data


class searchCharts(Resource):
    def post(self):
        args = task_post_args.parse_args()
        modKeyword, level = args['keyword'], int(args['level'])
        if(level == 4):
            query = """SELECT "Master_Table"."Level1", "Master_Table"."Level2", "Master_Table"."Level3", "Master_Table"."Level4" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" ILIKE any (array['% """+modKeyword+"""%', '"""+modKeyword+"""']) OR "Master_Table"."Level2" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level3" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level4" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) GROUP BY "Master_Table"."Level4", "Master_Table"."Level3", "Master_Table"."Level2", "Master_Table"."Level1" ORDER BY "Master_Table"."Level4", "Master_Table"."Level3", "Master_Table"."Level2", "Master_Table"."Level1" LIMIT 100"""

        elif(level == 3):
            query = """SELECT "Master_Table"."Level1", "Master_Table"."Level2", "Master_Table"."Level3" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level2" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level3" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) GROUP BY "Master_Table"."Level3", "Master_Table"."Level2", "Master_Table"."Level1" ORDER BY "Master_Table"."Level3", "Master_Table"."Level2", "Master_Table"."Level1" LIMIT 100"""

        elif(level == 2):
            query = """SELECT "Master_Table"."Level1", "Master_Table"."Level2" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level2" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) GROUP BY "Master_Table"."Level2", "Master_Table"."Level1" ORDER BY "Master_Table"."Level2", "Master_Table"."Level1" LIMIT 100"""

        else :
            query = """SELECT "Master_Table"."Level1", "Master_Table"."Level2", "Master_Table"."Level3", "Master_Table"."Level4" FROM economics_v202."Master_Table" WHERE "Master_Table"."Level1" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level2" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level3" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) OR "Master_Table"."Level4" ILIKE any (array['% """+modKeyword+"""', '"""+modKeyword+"""']) GROUP BY "Master_Table"."Level4", "Master_Table"."Level3", "Master_Table"."Level2", "Master_Table"."Level1" ORDER BY "Master_Table"."Level4", "Master_Table"."Level3", "Master_Table"."Level2", "Master_Table"."Level1" LIMIT 100"""
        print(level , query)
        data = cursor.execute(query)
        return data



api.add_resource(getDashboards, '/dashboards')
api.add_resource(getDashboardImages,'/dashimages')
api.add_resource(getMainPageCards,'/getMainPageCards')
api.add_resource(getLevel3andPath, '/Level3')
api.add_resource(getLevel4, '/Level4')
api.add_resource(getRates, '/rates')
api.add_resource(getLevelAll3, '/LevelAll3')
api.add_resource(getDataForLevel3Key, '/Level3Right')
api.add_resource(getTableData, '/TableData')
api.add_resource(getChartsData, '/getChartsData')
api.add_resource(getLevel3Filters, '/Level3Filters')
api.add_resource(searchCharts, '/searchCharts')
api.add_resource(level2function, '/Level2')
api.add_resource(suggestDataset, '/suggestDataset')


if __name__ == "__main__":
    app.run(debug = True)   