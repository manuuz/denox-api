from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
from bson.json_util import dumps

from .calculus import metricsCalculus, returnMetrics
from .database import connection_database

## classe de tela principal
class MainHandler(RequestHandler):
    def get(self):
        self.write("Hello World! Esse é a API da Emanuelle para o teste técnico da Denox!")

## endpoints do cálculo de métricas
class MetricsCalculusHandler(RequestHandler):
    def post(self):
        #procura as principais informais por meio das colunas
        serial = self.get_argument('serial', None)
        datahora_inicio = self.get_argument('datahora_inicio', None)
        datahora_fim = self.get_argument('datahora_fim', None)
  	    
        # passa os parametros para a função de retorno das métricas
        data = returnMetrics(serial, datahora_inicio, datahora_fim)

        # calcula os valores agora filtrados
        payload = metricsCalculus(data)

        # conexão com banco de dados para salvar a análise feita
        db = connection_database()
        results = db['resultados_emanuelle']
        results.insert_one(payload)

        self.write(dumps(payload))

    
    def get(self):
        self.clear()
        self.set_status(400)

# endpoints do retorno de métricas
class MetricsReturnHandler(RequestHandler):
    def get(self):
        # conecta com banco de dados e procura a ultima analise feita
        db = connection_database()
        results = db.resultados_emanuelle.find({}, {'_id': False})
        lastResult = results.sort("_id", -1).limit(1)

        # output do dado encontrado
        self.write(dumps(lastResult))


## make_app com url das APIs
def make_app():
    urls = [
        ("/", MainHandler),
        ("/api/calcula_metricas", MetricsCalculusHandler),
        ("/api/retorna_metricas", MetricsReturnHandler)
    ]
    return Application(urls, debug=True)

if __name__ == "__main__":
    app = make_app()
    app.listen(3000)
    IOLoop.current().start()
