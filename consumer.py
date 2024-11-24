import Pyro4
import time


def connect_to_leader():
    """
    Tenta conectar-se ao líder registrado no serviço de nomes.
    Retorna o proxy Pyro do líder se bem-sucedido.
    """
    try:
        print("Conectando ao líder...")
        return Pyro4.Proxy("PYRONAME:Líder-Epoca1")
    except Exception as e:
        print(f"Erro ao conectar ao líder: {e}")
        return None


def consume_committed_log(leader):
    """
    Solicita ao líder os dados confirmados e os exibe.
    """
    try:
        committed_log = leader.get_committed_log()

        if not committed_log:
            print("Nenhum dado confirmado disponível.")
        else:
            print("Dados confirmados consumidos:")
            for entry in committed_log:
                print(f" - {entry['data']} (epoca: {entry['epoca']}, offset: {entry['offset']})")
    except Exception as e:
        print(f"Erro ao consumir dados: {e}")
        if "quorum_failure" in str(e).lower():
            print("Aviso: O cluster está em reconfiguração. Tentando novamente em breve.")


def main():
    leader = connect_to_leader()

    if not leader:
        print("Erro: Não foi possível conectar ao líder. Encerrando o consumidor.")
        return

    print("Consumidor conectado ao líder. Aguardando dados confirmados...")

    while True:
        if not leader:
            print("Tentando reconectar ao líder...")
            leader = connect_to_leader()
            if not leader:
                print("Erro: Ainda não foi possível conectar ao líder. Tentando novamente em breve.")
                time.sleep(5)
                continue

        # Consome os dados confirmados
        consume_committed_log(leader)

        # Aguarda um intervalo antes de tentar consumir novamente
        time.sleep(10)


if __name__ == "__main__":
    main()
