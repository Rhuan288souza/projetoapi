from flask import Flask,jsonify,request
from flask_pymongo import PyMongo
from bson.json_util import dumps
from bson.objectid import ObjectId
import pandas as pd
import json

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
    Mês com menor quantidade de ocorrências de determinado crime, estado e ano
  
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


'''
    Quantidade de ocorrências por ano de um determinado estado
    db.ocorrencias.aggregate([{$match:{UF:"Acre"}},{$group:{_id:"$Ano",total:{$sum:"$Ocorrências"}}},])
    Exemplo : mesMenorQtdOcorrenciasEstado?UF=Acre&TipoCrime=Estupro&Ano=2015
'''


'''
    Quantidade anual de ocorrências por estado
    Soma as quantidades de ocorrência de determinado crime
    Exemplo : qtdOcorrenciasPorAnoEstado?Ano=2015
    @app.route('/qtdOcorrenciasPorAnoEstado')
    def qtdCrimePorAnoEstado():
    TipoCrime = request.args.get('TipoCrime', type = str)
    retorno = mongo.db.ocorrencias.aggregate([{'$match':{'TipoCrime':TipoCrime}},{'$group':{'_id':'$UF','total':{'$sum':"$Ocorrências"}}},])
    resp = dumps(retorno)
    return resp
'''




'''
    Todos os sobre a ocorrência de determinado crime por estado
    Exemplo : ocorrenciasPorEstado?TipoCrime=Estupro
'''

@app.route('/ocorrenciasPorEstado')
def ocorrenciasPorEstado():
    TipoCrime = request.args.get('TipoCrime', type = str)
    retorno = mongo.db.ocorrencias.aggregate([{'$match':{'TipoCrime':TipoCrime}}])
    resp = dumps(retorno)
    return resp


'''
    Tipo de crime com maior quantidade de ocorrências por estado em determinado ano
    Exemplo : ocorrenciasEstado?UF=Acre&TipoCrime=Estupro&Mes=janeiro&Ano=2015
'''
@app.route('/tipoCrimeMaiorOcorrenciasEstado')
def tipoCrimeMaiorOcorrenciasEstado():
    UF = request.args.get('UF', type = str)
    Ano = request.args.get('Ano', type = int)
    ocorrencias = mongo.db.ocorrencias.find({'UF': UF,'Ano':Ano},{'UF':1,'Mês':1,'TipoCrime':1,'_id':0,'Ocorrências':1}).sort([('Ocorrências',-1)]).limit(1)
    resp = dumps(ocorrencias)
    return resp

    '''
    Tipo de crime com maior quantidade de ocorrências por estado em determinado ano
    Exemplo : ocorrenciasEstado?UF=Acre&TipoCrime=Estupro&Mes=janeiro&Ano=2015
'''
@app.route('/tipoCrimeMenorOcorrenciasEstado')
def tipoCrimeMenorOcorrenciasEstado():
    UF = request.args.get('UF', type = str)
    Ano = request.args.get('Ano', type = int)
    ocorrencias = mongo.db.ocorrencias.find({'UF': UF,'Ano':Ano},{'UF':1,'Mês':1,'TipoCrime':1,'_id':0,'Ocorrências':1}).sort([('Ocorrências',1)]).limit(1)
    resp = dumps(ocorrencias)
    return resp



'''
    Retorna o número de vítimas de um determinado crime em um mês específico do ano em uma UF:
    Exemplo : ocorrenciasEstado?UF=Acre&TipoCrime=Estupro&Mes=janeiro&Ano=2015
'''
@app.route('/vitimasEstado')
def vitimas():
    UF = request.args.get('UF', type = str)
    TipoCrime = request.args.get('TipoCrime', type = str)
    Mes = request.args.get('Mes', type = str)
    Ano = request.args.get('Ano', type = int)
    vitimas = mongo.db.vitimas.find({'UF':UF,'TipoCrime':TipoCrime,'Mês':Mes,'Ano':Ano},{'Vítimas':1,'_id':0})
    resp = dumps(vitimas)
    return resp


'''
    Mês com maior quantidade de vítimas de determinado crime, estado e ano
  
    Exemplo : mesMaiorQtdOcorrenciasEstado?UF=Acre&TipoCrime=Estupro&Ano=2015
'''
@app.route('/mesMaiorQtdVitimasEstado')
def mesMaiorQtdVitimasEstado():
    UF = request.args.get('UF', type = str)
    TipoCrime = request.args.get('TipoCrime', type = str)
    Ano = request.args.get('Ano', type = int)
    vitimas = mongo.db.vitimas.find({'UF': UF,'TipoCrime':TipoCrime,'Ano':Ano},{'UF':1,'Mês':1,'Vítimas':1,'_id':0}).sort([('Vítimas',-1)]).limit(1)
    resp = dumps(vitimas)
    return resp

    '''
    Mês com menor quantidade de vítimas de determinado crime, estado e ano
  
    Exemplo : mesMaiorQtdOcorrenciasEstado?UF=Acre&TipoCrime=Estupro&Ano=2015
'''
@app.route('/mesMenorQtdVitimasEstado')
def mesMenorQtdVitimasEstado():
    UF = request.args.get('UF', type = str)
    TipoCrime = request.args.get('TipoCrime', type = str)
    Ano = request.args.get('Ano', type = int)
    vitimas = mongo.db.vitimas.find({'UF': UF,'TipoCrime':TipoCrime,'Ano':Ano},{'UF':1,'Mês':1,'Vítimas':1,'_id':0}).sort([('Vítimas',1)]).limit(1)
    resp = dumps(vitimas)
    return resp

''' 
    ROTAS PARA MUNICIPIOS COLLECTION: localhost:5000/municipios/
        Município |	Sigla UF | Região | Mês/Ano | Vítimas
'''

'''
    /municipios/vitimas -> Exibir todos os municípios e número de vítimas
    /municipios/vitimas?uf=<SIGLA_UF> -> Exibir todos os municípios e número de vítimas a partir da UF
'''
@app.route('/municipios/vitimas')
def filtraMunicipiosPorEstado():
    uf = request.args.get('uf', type = str)
    municipios = pd.read_csv('./datasets/municipio.csv') # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao', 'mes_ano', 'vitimas'] # Renomeia colunas (padronização)
    if (uf != None): # Se houver na query o parâmetro uf, faça o filtro
        municipios = municipios.query(f"estado == '{uf}'") # Filtra por estado
    municipios = municipios.groupby('municipio').sum('vitimas') # Agrupa por munpicípio e soma as vítimas

    print(municipios.head()) # A função head retorna os cinco primeiros (só para debugging no terminal)

    return municipios['vitimas'].to_json()

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

