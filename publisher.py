import Pyro4

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

def main():
    leader = connect_to_leader()

    while True:
        data = input("Insira os dados para publicar (ou 'exit' para sair): ")
        if data.lower() == "exit":
            break

        if not leader:
            print("Líder não disponível. Tentando reconectar...")
            leader = connect_to_leader()
            if not leader:
                print("Erro: Não foi possível conectar ao líder. Tente novamente mais tarde.")
                continue

        try:
            # Envia dados ao líder
            print("Enviando dados ao líder...")
            confirmation = leader.append_entry(data)

            if confirmation:
                print(f"Líder confirmou: Dados '{data}' replicados com sucesso!")
            else:
                print(f"Erro: Não foi possível confirmar a replicação dos dados.")
        except Exception as e:
            print(f"Erro ao enviar dados ao líder: {e}")
            if "quorum_failure" in str(e).lower():
                print("Aviso: O cluster está sendo reconfigurado devido a falhas de votantes. Tente novamente em breve.")
            leader = None  # Reset para forçar reconexão no próximo loop

if __name__ == "__main__":
    main()

