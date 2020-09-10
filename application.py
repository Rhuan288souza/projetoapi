from flask import Flask, jsonify, request
import pandas as pd
import json

application = app = Flask(__name__)


'''
    /estado/vitimas -> Rota de teste que retorna todos os registros da coleção vítimas
    
   

    exemplo??
'''


@app.route('/estado/vitimas')
def estadoVitimasDump():
    vitimas = pd.read_csv('./datasets/estado_vitimas.csv')
    vitimas.columns = ['uf', 'crime', 'ano', 'mes', 'vitimas']
    return vitimas.to_json(orient='records')


'''
    /estado/ocorrencias -> Retorna o número de ocorrências de um determinado crime em um mês específico do ano em uma UF:
        Parâmetros: 
            -tipo_crime-obrigatorio
            -mes-obrigatorio
            -ano-obrigatorio
            -uf-obrigatorio
        Exemplo:
            /estado/ocorrencias?uf=Maranhão&crime=Estupro&ano=2015&mes=janeiro
            
           
'''


@app.route('/estado/ocorrencias')
def FiltraEstadoOcorrencias():
    ocorrencias = pd.read_csv('./datasets/estado_ocorrencias.csv')
    ocorrencias.columns = ['uf', 'crime', 'ano', 'mes', 'ocorrencias']
    uf = request.args.get('uf', type=str)
    crime = request.args.get('crime', type=str)
    mes = request.args.get('mes', type=str)
    ano = request.args.get('ano', type=str)
    ocorrencias = ocorrencias.query(
        f"uf == '{uf}'") if (uf != None) else ocorrencias
    ocorrencias = ocorrencias.query(f"crime == '{crime}'") if (
        crime != None) else ocorrencias
    ocorrencias = ocorrencias.query(f"mes == '{mes}'") if (
        mes != None) else ocorrencias
    ocorrencias = ocorrencias.query(f"ano == '{ano}'") if (
        ano != None) else ocorrencias
    return ocorrencias.to_json(orient='records')


'''
    Top X de estados com maior número ou menor número de ocorrências por crime em um determinado mês e ano
    Por padrão mostra em ordem descendente
    Para ordem ascendente Ordem = asc
    Exemplo : rankingocorrenciasEstado?TipoCrime=Estupro&Mes=janeiro&Ano=2015&Qtd=10 
    
   
'''


@app.route('/rankingocorrenciasEstado')
def rankingOcorrencias():
    tipo_crime = request.args.get('tipo_crime', type=str)
    mes = request.args.get('mes', type=str)
    ano = request.args.get('ano', type=int)
    quantidade = request.args.get('qtd', type=int)
    ordem = request.args.get('ordem', type=str)

    return False


'''
    Mês com maior quantidade de ocorrências de determinado crime, estado e ano
  
    Exemplo : mesMaiorQtdOcorrenciasEstado?UF=Acre&TipoCrime=Estupro&Ano=2015
    
    
'''


@app.route('/mesMaiorQtdOcorrenciasEstado')
def mesQtdMaiorOcorrencias():
    uf = request.args.get('uF', type=str)
    tipo_crime = request.args.get('Tipo_crime', type=str)
    ano = request.args.get('ano', type=int)

    return False


'''
    Mês com menor quantidade de ocorrências de determinado crime, estado e ano
  
    Exemplo : mesMenorQtdOcorrenciasEstado?UF=Acre&TipoCrime=Estupro&Ano=2015
    
    
'''


@app.route('/mesMenorQtdOcorrenciasEstado')
def mesQtdMenorOcorrencias():
    uf = request.args.get('uf', type=str)
    tipo_crime = request.args.get('tipo_crime', type=str)
    ano = request.args.get('ano', type=int)

    return False


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
    tipo_crime = request.args.get('tipo_crime', type=str)

    return False


'''
    Tipo de crime com maior quantidade de ocorrências por estado em determinado ano
    Exemplo : tipoCrimeMaiorOcorrenciasEstado?UF=Acre&Ano=2015 
    
   
'''


@app.route('/tipoCrimeMaiorOcorrenciasEstado')
def tipoCrimeMaiorOcorrenciasEstado():
    uf = request.args.get('uf', type=str)
    ano = request.args.get('ano', type=int)

    return False


'''
    Tipo de crime com menor quantidade de ocorrências por estado em determinado ano 
    Parâmetros:
    -ano-obrigatorio

    Exemplo : /tipoCrime/menorocorrencias/estado?ano=2017&menor=1   
    
    O código abaixo esta retornado uma lista vazia: []
'''


@app.route('/tipoCrime/menorocorrencias/estado')
def tipo_crime_menor_ocorrencias_Estado():
    estados = pd.read_csv('./datasets/estado_vitimas.csv') # Carrega dataset
    estados.columns = ['uf', 'tipo_crime', 'ano', 'mes', 'vitimas'] # Renomeia colunas (padronização)
    tipo_crime = request.args.get('tipo_crime', type=str) # Crime cometido
    uf = request.args.get('uf', type=str) # Estado (opcional)
    ano = request.args.get('ano', type=str) # Ano (opcional)
    estados = estados.query(f"tipo_crime == '{tipo_crime}'") # Filtra a UF
    estados = estados.query(f"uf == '{uf}'") # Filtra a UF
    estados = estados.query(f"ano == '{ano}'") # Filtra o ano
    estados = estados.groupby('tipo_crime').sum('vitimas').sort_values('vitimas', ascending=False) # Agrupa por tipo de crime e soma as vítimas e retorna numa lista descen
    return estados.to_json(orient='records')
   
   
   
    


'''
    /estado/vitimas/ranking_crime -> Retorna o número de vítimas de um determinado crime em um mês específico do ano em uma UF:
    Parâmetros: 
        - tipo_crime - obrigatorio
        - uf - obrigatorio
        - ano - obrigatorio

    Exemplo:
        /estado/vitimas/ranking_crime?tipo_crime=Homicídio%20doloso&uf=Bahia&ano=2017 
'''
# @app.route('/estado/vitimas/')


@app.route('/estado/vitimas/ranking_crime')
def ranking_crime_vitimas():
    estados = pd.read_csv('./datasets/estado_vitimas.csv')  # Carrega dataset
    estados.columns = ['uf', 'tipo_crime', 'ano', 'mes',
                       'vitimas']  # Renomeia colunas (padronização)
    tipo_crime = request.args.get('tipo_crime', type=str)  # Crime cometido
    uf = request.args.get('uf', type=str)  # Estado (opcional)
    ano = request.args.get('ano', type=str)  # Ano (opcional)
    estados = estados.query(f"tipo_crime == '{tipo_crime}'")  # Filtra a UF
    estados = estados.query(f"uf == '{uf}'")  # Filtra a UF
    estados = estados.query(f"ano == '{ano}'")  # Filtra o ano
    # Agrupa por tipo de crime e soma as vítimas
    estados = estados.groupby('tipo_crime').sum('vitimas')
    payload = {
        'estado': uf,
        'ano': ano,
        'crime': tipo_crime,
        'vitimas': str(estados['vitimas'][tipo_crime])
    }
    return payload  # Retorna o primeiro (maior/menor dependendo da selecao)


'''
    /estado/vitimas/crime_mes -> Mês com (menor ou maior) quantidade de vítimas de determinado crime. Estado, Ano (opcionais)
    Parâmetros: 
        - tipo_crime - obrigatorio
        - uf - opcional
        - ano - opcional
        - maior - opcional (se for 1, retorna o maior)
    Exemplo:
        /estado/vitimas/crime_mes?tipo_crime=Homicídio%20doloso&uf=Bahia&ano=2017&maior=1   
'''
# @app.route('/mesMenorQtdVitimasEstado')


@app.route('/estado/vitimas/crime_mes')
def mes_menor_crime_vitimas():
    estados = pd.read_csv('./datasets/estado_vitimas.csv')  # Carrega dataset
    estados.columns = ['uf', 'tipo_crime', 'ano', 'mes',
                       'vitimas']  # Renomeia colunas (padronização)
    # Se houver 'maior', retorna o mes com a maior quantidade de vítimas
    maior = request.args.get('maior', type=str)
    tipo_crime = request.args.get('tipo_crime', type=str)  # Crime cometido
    uf = request.args.get('uf', type=str)  # Estado (opcional)
    ano = request.args.get('ano', type=str)  # Ano (opcional)
    estados = estados.query(
        f"tipo_crime == '{tipo_crime}'")  # Filtra os crimes
    estados = estados.sort_values('vitimas', ascending=False) if (
        maior == '1') else estados  # Pesquisa se foi requisitado o maior
    estados = estados.query(f"uf == '{uf}'") if (
        uf != None) else estados  # Se houver estados filtra
    estados = estados.query(f"ano == '{ano}'") if (
        ano != None) else estados  # Se houver ano filtra
    # Retorna o primeiro (maior/menor dependendo da selecao)
    return estados[0:1].to_json(orient='records')


''' 
    ROTAS PARA MUNICIPIOS COLLECTION: localhost:5000/municipios/
        Município |	Sigla UF | Região | Mês/Ano | Vítimas
'''

'''
    /municipios/ -> Retorna todos os registros
        /municipios/
'''


@app.route('/municipios/')
def dump_registros_municipios():
    municipios = pd.read_csv(
        './datasets/municipio_vitimas.csv')  # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao',
                          'mes_ano', 'vitimas']  # Renomeia colunas (padronização)
    return municipios.to_json(orient='records')


'''
    /municipios/vitimas -> Exibir todos os municípios e número total de vítimas
    Parâmetros: 
            -uf-não obrigatório
        Exemplo:
            /municipios/vitimas?uf=MA
'''


@app.route('/municipios/vitimas')
def total_vitimas_municipio():
    municipios = pd.read_csv(
        './datasets/municipio_vitimas.csv')  # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao',
                          'mes_ano', 'vitimas']  # Renomeia colunas (padronização)
    # Carrega o parâmetro 'uf' (não-obrigatório)
    uf = request.args.get('uf', type=str)
    municipios = municipios.query(f"estado == '{uf}'") if (
        uf != None) else municipios  # Verifica se 'uf' existe e faz o filtro
    municipios = municipios.groupby('municipio').sum(
        'vitimas')  # Agrupa por munpicípio e soma as vítimas
    return municipios['vitimas'].to_json()


'''
    /municipios/periodo -> Quantidade de vítimas em determinado município de acordo com Mês/Ano.
    Parâmetros: 
            ? municipio & periodo & only_ano
        Exemplo:
            /municipios/periodo?municipio=Tutóia&periodo=jan/18&only_ano=true
'''


@app.route('/municipios/periodo')
def total_vitimas_municipio_periodo():
    municipios = pd.read_csv(
        './datasets/municipio_vitimas.csv')  # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao',
                          'mes_ano', 'vitimas']  # Renomeia colunas (padronização)
    # Carrega o parâmetro 'município' (obrigatorio)
    municipio = request.args.get('municipio', type=str)
    # Carrega o parâmetro 'periodo' (obrigatorio)
    periodo = request.args.get('periodo', type=str)
    # Carrega o parâmetro 'periodo' (obrigatorio)
    only_ano = request.args.get('only_ano', type=str)
    if (municipio != None and periodo != None):  # Verifica se os dois existem
        if (only_ano != None and only_ano == '1'):  # Verifica se o usuario quer todos os meses do ano
            municipios['ano'] = municipios['mes_ano'].str.extract(
                r'([0-9]{2}$)')  # Cria uma nova coluna 'ano' em todos os registro
            ano = periodo.split('/')[1]  # Captura o ano do periodo desejado
            municipios = municipios.query(
                f"municipio == '{municipio}' & ano == '{ano}'")  # Faz o filtro
            del municipios['ano']  # Apaga a coluna 'ano'
        else:
            municipios = municipios.query(
                f"municipio == '{municipio}' & mes_ano == '{periodo}'")  # Faz o filtro
    else:
        # Parâmetros faltantes
        return bad_request('Insira o município/período')
    return municipios.to_json(orient='records')


'''
    /municipios/ranking_estado -> Município(s) com os 5 maiores/menores números de vítimas de cada estado.
    Parâmetros: 
            -uf-obrigatorio
        Exemplo:
            /municipios/ranking_estado?uf=MA
            
            
'''


@app.route('/municipios/ranking_estado')
def ranking_vitimas_municipios_uf():
    municipios = pd.read_csv(
        './datasets/municipio_vitimas.csv')  # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao',
                          'mes_ano', 'vitimas']  # Renomeia colunas (padronização)
    # Carrega o parâmetro 'estado' (obrigatorio)
    uf = request.args.get('uf', type=str)
    if (uf != None):  # Verifica se os dois existem
        municipios = municipios.query(f"estado == '{uf}'")  # Faz o filtro
        municipios = municipios.groupby('municipio').sum('vitimas')
        municipios = municipios.sort_values('vitimas', ascending=False)
        maiores = municipios.iloc[0:10]['vitimas']
        menores = municipios.iloc[-10:-1]['vitimas']
        result = {'maiores': maiores, 'menores': menores}
    else:
        return bad_request('Insira o estado')  # Parâmetros faltantes
    return dumps(result)


'''
    /municipios/ranking_regiao -> Ranking das regiões com números de vítimas.
'''


@app.route('/municipios/ranking_regiao')
def ranking_vitimas_municipios_regiao():
    municipios = pd.read_csv(
        './datasets/municipio_vitimas.csv')  # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao',
                          'mes_ano', 'vitimas']  # Renomeia colunas (padronização)
    municipios = municipios.groupby('regiao').sum('vitimas')
    return municipios['vitimas'].to_json()


'''
    /municipios/ranking_periodo_municipio -> O Mês/Ano ou Ano que apresentou o maior número de vítimas em cada município.
'''


@app.route('/municipios/ranking_periodo_municipio')
def ranking_vitimas_periodo_municipio():
    municipios = pd.read_csv(
        './datasets/municipio_vitimas.csv')  # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao',
                          'mes_ano', 'vitimas']  # Renomeia colunas (padronização)
    # Cria um dataframe para receber os resultados
    results = pd.DataFrame(columns=['municipio', 'mes_ano', 'vitimas'])
    for municipio in municipios['municipio'].unique().tolist():
        # Infelizmente, os municípios que tiverem uma aspas simples será descartado, precisaria tratar antes do filtro
        if (municipio.find('\'') != -1):
            continue
        meses_municipio = municipios.query(f"municipio == '{municipio}'")
        meses_municipio = meses_municipio.sort_values(
            'vitimas', ascending=False)
        row = {'municipio': municipio,
               'mes_ano': meses_municipio.iloc[0]['mes_ano'], 'vitimas': meses_municipio.iloc[0]['vitimas']}
        results = results.append(row, ignore_index=True)

    return results.to_json(orient='records')


'''
    /municipios/livre_vitimas -> Municípios que não apresentaram vítimas em determinado Mês/Ano.
    Parâmetros: 
        -mes_ano-obrigatorio 
    Exemplo: 
        /municipios/livre_vitimas?mes_ano=jan/18
'''


@app.route('/municipios/livre_vitimas')
def livre_vitimas():
    municipios = pd.read_csv(
        './datasets/municipio_vitimas.csv')  # Carrega dataset
    municipios.columns = ['municipio', 'estado', 'regiao',
                          'mes_ano', 'vitimas']  # Renomeia colunas (padronização)
    # Carrega o parâmetro 'mes_ano' (não obrigatorio)
    mes_ano = request.args.get('mes_ano', type=str)
    municipios = municipios.query(f"vitimas == 0 & mes_ano == '{mes_ano}'") if (
        mes_ano != None) else municipios.query('vitimas == 0')
    return municipios['municipio'].to_json(orient='records')


'''
    /municipios/recordes_vitimas -> Municípios que apresentaram o maior número de vítimas de acordo com o Mês/Ano.
    Parâmetros:
    -mes_ano-obrigatorio
'''


@app.route('/municipios/recordes_vitimas')
def recordes_vitimas():
    municipios = pd.read_csv('./datasets/municipio_vitimas.csv')
    municipios.columns = ['municipio', 'estado',
                          'regiao', 'mes_ano', 'vitimas']
    results = pd.DataFrame(columns=['mes_ano', 'municipio', 'vitimas'])
    for mes_ano in municipios['mes_ano'].unique().tolist():
        meses_municipio = municipios.query(f"mes_ano == '{mes_ano}'")
        meses_municipio = meses_municipio.sort_values(
            'vitimas', ascending=False)
        row = {'mes_ano': mes_ano,
               'municipio': meses_municipio.iloc[0]['municipio'], 'vitimas': meses_municipio.iloc[0]['vitimas']}
        results = results.append(row, ignore_index=True)
    return results.to_json(orient='records')


@app.errorhandler(404)
def not_found(error=None):
    message = {
        'status': 404,
        'message': 'A seguinte url não foi encontrada:' + request.url
    }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


def bad_request(mensagem):
    message = {
        'status': 400,
        'message': 'Bad Request: ' + mensagem
    }
    resp = jsonify(message)
    resp.status_code = 400
    return resp


if __name__ == "__main__":
    app.run(debug=True)
