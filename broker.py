import Pyro4
from time import sleep

@Pyro4.expose
class Broker:
    def __init__(self, broker_id, role, cluster_size):  # Corrigido para __init__
        self.broker_id = broker_id
        self.role = role  # líder, votante ou observador
        self.cluster_size = cluster_size
        self.log = []  # log para armazenar os dados
        self.connections = {}  # Registro de outros brokers conectados (usado pelo líder)

    # Votantes e observadores se registram no líder
    def register_broker(self, broker_id, role):
        if self.role == "leader":
            self.connections[broker_id] = role
            print(f"Broker {broker_id} com função '{role}' registrado no líder.")
            return True
        return False

    # Publicadores enviam dados para o líder, que replica nos votantes
    def append_entry(self, data):
        if self.role == "leader":
            self.log.append(data)
            print(f"Dados recebidos pelo líder: {data}. Replicando para os votantes...")
            for broker_id, role in self.connections.items():
                if role == "voter":
                    uri = Pyro4.Proxy(f"PYRONAME:broker.{broker_id}")
                    uri.replicate_entry(data)
            return "Dados registrados e replicados com sucesso."
        else:
            return "Apenas o líder pode receber dados do publicador"

    # Votantes recebem e replicam dados do líder
    def replicate_entry(self, data):
        if self.role == "voter":
            self.log.append(data)
            print(f"Broker {self.broker_id}: Dados replicados: {data}")
            return True
        return False

    # Consumidor requisita dados do broker (apenas do líder ou votantes)
    def consume_data(self):
        if self.role in ["leader", "voter"]:
            if self.log:
                return self.log.pop(0)
            else:
                return "Nenhum dado disponível no log"
        return "Observadores não armazenam dados."

# Função para inicializar o broker e registrá-lo no serviço de nomes do PyRO.
def start_broker(broker_id, role, cluster_size):  # Corrigido: função não deve estar dentro da classe
    broker = Broker(broker_id, role, cluster_size)
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()

    if role == "leader":
        uri = daemon.register(broker)
        ns.register("Líder-Epoca1", uri)  # Registra o líder como 'Líder-Epoca1'
        print(f"Líder registrado como 'Líder-Epoca1' com URI {uri}.")
    else:
        uri = daemon.register(broker)
        ns.register(f"broker.{broker_id}", uri)
        leader = Pyro4.Proxy("PYRONAME:Líder-Epoca1")  # Removido espaço extra no nome
        leader.register_broker(broker_id, role)
        print(f"Broker {broker_id} registrado como '{role}' e conectado ao líder.")

    daemon.requestLoop()


if __name__ == "__main__":
    import sys
    broker_id = int(sys.argv[1])
    role = sys.argv[2]  # líder, votante ou observador
    cluster_size = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    start_broker(broker_id, role, cluster_size)
