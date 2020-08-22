from flask import Flask,jsonify,request
from flask_pymongo import PyMongo
from bson.json_util import dumps
from bson.objectid import ObjectId


app = Flask(__name__)

app.config['MONGO_URI'] = "mongodb+srv://rhuan:mongo14071f@cluster0.sxffn.mongodb.net/principal?retryWrites=true&w=majority"
mongo = PyMongo(app)


@app.route('/uf')
def uf():
    uf = mongo.db.vitimas.find()
    resp = dumps(uf)
    return resp

@app.errorhandler(404)
def not_found (error=None):
    message = {
        'status' : 404,
        'message' : 'Não Encontrado' + request.url
    }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


if __name__==  "__main__":
    app.run(debug=True)

