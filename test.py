from flask import Flask,jsonify,request
from flask_pymongo import PyMongo
from bson.json_util import dumps
from bson.objectid import ObjectId


app = Flask(__name__)

app.config['MONGO_URI'] = "mongodb+srv://rhuan:mongo14071f@cluster0.sxffn.mongodb.net/principal?retryWrites=true&w=majority"
mongo = PyMongo(app)



#Rota de teste que retorna todos os registros da coleção vítimas
@app.route('/uf')
def uf():
    uf = mongo.db.vitimas.find()
    resp = dumps(uf)
    return resp


'''
    Retorna o número de ocorrências de um determinado crime em um mês específico do ano em uma UF:
    Exemplo : ocorrenciasEstado?UF=Acre&TipoCrime=Estupro&Mes=janeiro&Ano=2015
'''
@app.route('/ocorrenciasEstado')
def ocorrencias():
    UF = request.args.get('UF', type = str)
    TipoCrime = request.args.get('TipoCrime', type = str)
    Mes = request.args.get('Mes', type = str)
    Ano = request.args.get('Ano', type = int)
    ocorrencias = mongo.db.ocorrencias.find({'UF':UF,'TipoCrime':TipoCrime,'Mês':Mes,'Ano':Ano},{'Ocorrências':1,'_id':0})
    resp = dumps(ocorrencias)
    return resp


'''
    Top X de estados com maior número ou menor número de ocorrências por crime em um determinado mês e ano
    Por padrão mostra em ordem descendente
    Para ordem ascendente Ordem = asc
    Exemplo : rankingocorrenciasEstado?TipoCrime=Estupro&Mes=janeiro&Ano=2015&Qtd=10
'''


@app.route('/rankingocorrenciasEstado')
def rankingOcorrencias():
    TipoCrime = request.args.get('TipoCrime', type = str)
    Mes = request.args.get('Mes', type = str)
    Ano = request.args.get('Ano', type = int)
    Quantidade = request.args.get('Qtd',type = int)
    Ordem = request.args.get('Ordem',type = str)
    i = 0
    i =1 if Ordem=="asc" else -1

    ocorrencias = mongo.db.ocorrencias.find({'TipoCrime':TipoCrime,'Mês':Mes,'Ano':Ano},{'UF':1,'Ocorrências':1,'_id':0}).sort([('Ocorrências',i)]).limit(Quantidade)
    resp = dumps(ocorrencias)
    return resp



'''
    Mês com maior quantidade de ocorrências de determinado crime, estado e ano
  
    Exemplo : mesMaiorQtdOcorrenciasEstado?UF=Acre&TipoCrime=Estupro&Ano=2015
'''
@app.route('/mesMaiorQtdOcorrenciasEstado')
def mesQtdMaiorOcorrencias():
    UF = request.args.get('UF', type = str)
    TipoCrime = request.args.get('TipoCrime', type = str)
    Ano = request.args.get('Ano', type = int)
    ocorrencias = mongo.db.ocorrencias.find({'UF': UF,'TipoCrime':TipoCrime,'Ano':Ano},{'UF':1,'Mês':1,'Ocorrências':1,'_id':0}).sort([('Ocorrências',-1)]).limit(1)
    resp = dumps(ocorrencias)
    return resp



'''
    Mês com maior quantidade de ocorrências de determinado crime, estado e ano
  
    Exemplo : mesMenorQtdOcorrenciasEstado?UF=Acre&TipoCrime=Estupro&Ano=2015
'''
@app.route('/mesMenorQtdOcorrenciasEstado')
def mesQtdMenorOcorrencias():
    UF = request.args.get('UF', type = str)
    TipoCrime = request.args.get('TipoCrime', type = str)
    Ano = request.args.get('Ano', type = int)
    ocorrencias = mongo.db.ocorrencias.find({'UF': UF,'TipoCrime':TipoCrime,'Ano':Ano},{'UF':1,'Mês':1,'Ocorrências':1,'_id':0}).sort([('Ocorrências',1)]).limit(1)
    resp = dumps(ocorrencias)
    return resp







@app.errorhandler(404)
def not_found (error=None):
    message = {
        'status' : 404,
        'message' : 'A seguinte url não foi encontrada:' + request.url
    }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


if __name__==  "__main__":
    app.run(debug=True)

