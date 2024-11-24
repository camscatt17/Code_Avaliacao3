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
        self.heartbeat_interval = 10 # Intervalo máximo para receber um heartbeat (em segundos)

        if self.role == "leader":
            self.commit_index = -1  # Último índice confirmado
            self.voter_heartbeats = {}  # Inicializa o dicionário para monitorar heartbeats
        print(f"Broker {broker_id} iniciado com {role}.")

    def register_broker(self, broker_id, role, proxy):
        if self.role != "leader":
            raise Exception("Apenas o líder pode registrar outros brokers.")
        
        proxy_obj = Pyro4.Proxy(proxy)
        
        if role == "voter":
            self.voters[broker_id] = proxy_obj
            self.voter_heartbeats[broker_id] = time.time()  # Inicializa o registro de heartbeat
            print(f"Líder {self.broker_id}: Votante {broker_id} registrado.")
        elif role == "observer":
            self.observers.append(proxy_obj)
            print(f"Líder {self.broker_id}: Observador {broker_id} registrado.")
        else:
            print(f"Líder {self.broker_id}: Papel '{role}' desconhecido para broker {broker_id}.")
    
    def heartbeat(self):
        if self.role != "voter":
            raise Exception("Apenas votantes enviam heartbeats.")
        while True:
            try:
                self.leader_proxy.receive_heartbeat(self.broker_id)
                print(f"Votante {self.broker_id}: Heartbeat enviado ao líder.")
            except Exception as e:
                print(f"Votante {self.broker_id}: Falha ao enviar heartbeat: {e}")
            time.sleep(self.heartbeat_interval / 2)  # Envia heartbeat a cada metade do intervalo permitido
    
    def receive_heartbeat(self, voter_id):
        if self.role != "leader":
            raise Exception("Apenas o líder pode receber heartbeats.")
        self.voter_heartbeats[voter_id] = time.time()
        print(f"Líder {self.broker_id}: Heartbeat recebido de votante {voter_id}.")
     
    def monitor_voters(self):
        while True:
            current_time = time.time()
            unavailable_voters = []
            for voter_id, last_heartbeat in self.voter_heartbeats.items():
                if current_time - last_heartbeat > self.heartbeat_interval:
                    unavailable_voters.append(voter_id)

            for voter_id in unavailable_voters:
                print(f"Líder {self.broker_id}: Votante {voter_id} indisponível.")
                del self.voters[voter_id]
                del self.voter_heartbeats[voter_id]

                # Verifica se ainda há quorum suficiente
                if len(self.voters) < (len(self.voters) // 2) + 1:
                    self.promote_observer_to_voter()

            time.sleep(2)  # Intervalo de monitoramento

    def promote_observer_to_voter(self):
        if self.observers:
            new_voter_proxy = self.observers.pop(0)
            new_voter_id = f"voter-{len(self.voters) + 1}"  # Cria um ID único para o novo votante
            self.voters[new_voter_id] = new_voter_proxy
            self.voter_heartbeats[new_voter_id] = time.time()

            print(f"Líder {self.broker_id}: Observador promovido a votante: {new_voter_id}")

            # Notifica os demais votantes
            for voter in self.voters.values():
                voter.notify_new_voter(new_voter_id)

            # Sincroniza os dados para o novo votante
            new_voter_proxy.synchronize_with_leader(self.broker_id)
    
    def notify_new_voter(self, new_voter_id):
        print(f"Votante {self.broker_id}: Notificado sobre novo votante: {new_voter_id}.")
    
    def synchronize_with_leader(self, leader_id):
        if self.role != "voter":
            raise Exception("Apenas votantes podem sincronizar com o líder.")
        try:
            data = self.leader_proxy.get_committed_log()
            self.log = data
            print(f"Votante {self.broker_id}: Dados sincronizados com o líder {leader_id}.")
        except Exception as e:
            print(f"Votante {self.broker_id}: Erro ao sincronizar com o líder: {e}")
    
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
            
            # Inicia o envio de heartbeats se for votante
            if role == "voter":
                threading.Thread(target=broker.heartbeat, daemon=True).start()
        except Exception as e:
            print(f"Erro ao conectar ao líder: {e}")
            return

    daemon.requestLoop()

if __name__ == "__main__":
    import sys
    broker_id = sys.argv[1]
    role = sys.argv[2]
    start_broker(broker_id, role)
