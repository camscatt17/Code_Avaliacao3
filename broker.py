import Pyro4
import time
import threading

@Pyro4.expose
class Broker:
    def __init__(self, broker_id, role): 
        self.broker_id = broker_id
        self.role = role  # líder, votante ou observador
        self.epoca = 1
        self.log = []  # Estrutura do log: [{"epoca": int, "offset": int, "data": str, "confirmed": bool}]
        self.leader_proxy = None  # Referência ao líder
        self.voters = {}  # Armazena os votantes (somente no líder)
        self.observers = []  # Lista de observadores conectados (somente no líder)
        self.confirmations = {}  # Armazena confirmações por offset

        if self.role == "leader":
            self.commit_index = -1  # Último índice confirmado
        print(f"Broker {broker_id} iniciado com {role} em um cluster de {broker_id} brokers.")

    def register_broker(self, broker_id, role, proxy):
        if self.role != "leader":
            raise Exception("Apenas o líder pode registrar outros brokers.")
        
        proxy_obj = Pyro4.Proxy(proxy)
        
        if role == "voter":
            self.voters[broker_id] = proxy_obj
            print(f"Líder {self.broker_id}: Votante {broker_id} registrado.")
        elif role == "observer":
            self.observers.append(proxy_obj)
            print(f"Líder {self.broker_id}: Observador {broker_id} registrado.")
        else:
            print(f"Líder {self.broker_id}: Papel '{role}' desconhecido para broker {broker_id}.")

    def append_entry(self, data):
        if self.role != "leader":
            raise Exception("Apenas o líder pode receber gravações.")

        # 1. Adiciona ao log do líder
        new_entry = {"epoca": self.epoca, "offset": len(self.log), "data": data, "confirmed": False}
        self.log.append(new_entry)
        print(f"Líder {self.broker_id}: Nova entrada adicionada ao log: {new_entry}")

        # Notifica votantes
        voters = list(self.voters.values())
        confirmations = []  # Lista para acompanhar confirmações

        for voter_proxy in voters:
            def notify_voter(voter):
                try:
                    voter.replicate_log(self.broker_id, len(self.log) - 1)
                    confirmations.append(True)
                except Exception as e:
                    print(f"Líder {self.broker_id}: Erro ao notificar votante: {e}")
                    confirmations.append(False)
            
            threading.Thread(target=notify_voter, args=(voter_proxy,)).start()

        # Aguarda as confirmações
        while len(confirmations) < len(voters):
            time.sleep(0.1)

        # Verifica se o quórum foi atingido
        if confirmations.count(True) >= (len(self.voters) // 2) + 1:
            self.log[-1]['confirmed'] = True
            print(f"Líder {self.broker_id}: Dados confirmados pelo quórum!")
            return True
        else:
            print(f"Líder {self.broker_id}: Falha ao atingir o quórum para confirmação.")
            return False

    def get_log_entry(self, log_offset):
        if self.role != "leader":
            raise Exception("Apenas o líder pode fornecer entradas do log.")
        
        if 0 <= log_offset < len(self.log):
            return self.log[log_offset]
        else:
            raise Exception(f"Offset {log_offset} inválido.")

    def replicate_log(self, leader_id, log_offset):
        if self.role != "voter":
            raise Exception("Apenas votantes podem replicar logs.")
        
        if not self.leader_proxy:
            raise Exception("Erro: Conexão com o líder não foi inicializada.")
        
        try:
            # Solicita os dados ao líder
            data_to_replicate = self.leader_proxy.get_log_entry(log_offset)
            if data_to_replicate:
                self.log.append(data_to_replicate)
                print(f"Votante {self.broker_id}: Dados replicados do líder: {data_to_replicate}")
                
                # Confirma replicação ao líder
                self.leader_proxy.confirm_replication(self.broker_id, log_offset)
        except Exception as e:
            print(f"Votante {self.broker_id}: Erro ao replicar log: {e}")

    def fetch_log(self, epoca, offset):
        if self.role != "leader":
            raise Exception("Apenas o líder pode responder requisições de log.")
        
        # Verifica a consistência do log
        if epoca != self.epoca or offset >= len(self.log):
            return {"error": "Inconsistência detectada", "truncate_to": len(self.log) - 1}
        
        entries = self.log[offset + 1:]
        return {"entries": entries}

    def confirm_replication(self, voter_id, log_offset):
        if self.role != "leader":
            raise Exception("Apenas o líder pode confirmar replicação.")

        print(f"Líder {self.broker_id}: Confirmação recebida de votante {voter_id} para offset {log_offset}.")
        self.confirmations.setdefault(log_offset, set()).add(voter_id)

        # Verifica se o quórum foi atingido para este offset
        if len(self.confirmations[log_offset]) >= (len(self.voters) // 2) + 1:
            self.log[log_offset]['confirmed'] = True
            print(f"Líder {self.broker_id}: Entrada no offset {log_offset} marcada como confirmada!")

    def get_committed_log(self):
        if self.role != "leader":
            raise Exception("Apenas o líder pode fornecer o log confirmado.")
        
        # Filtra apenas as entradas confirmadas
        committed_entries = [entry for entry in self.log if entry["confirmed"]]
        print(f"Líder {self.broker_id}: Enviando log confirmado ao consumidor: {committed_entries}")
        return committed_entries


# Função para inicializar o broker e registrá-lo no serviço de nomes do PyRO.
def start_broker(broker_id, role): 
    broker = Broker(broker_id, role)
    daemon = Pyro4.Daemon()  # Servidor do Pyro
    ns = Pyro4.locateNS()  # Localiza o serviço de nomes

    if role == "leader":
        uri = daemon.register(broker)
        ns.register("Líder-Epoca1", uri)
        print(f"Líder registrado como 'Líder-Epoca1' com URI {uri}.")
    else:
        uri = daemon.register(broker)
        ns.register(f"broker.{broker_id}", uri)
        
        try:
            leader_proxy = Pyro4.Proxy("PYRONAME:Líder-Epoca1")
            broker.leader_proxy = leader_proxy
            leader_proxy.register_broker(broker_id, role, uri)
            print(f"Broker {broker_id} registrado como '{role}' e conectado ao líder.")
        except Exception as e:
            print(f"Erro ao conectar ao líder: {e}")
            return

    daemon.requestLoop()

if __name__ == "__main__":
    import sys
    broker_id = sys.argv[1]
    role = sys.argv[2]
    start_broker(broker_id, role)
