import Pyro4

def main():
    # Conecta-se ao líder via serviço de nomes
    leader = Pyro4.Proxy("PYRONAME:Líder-Epoca1")
    
    print("Consumidor conectado ao líder. Aguardando dados confirmados...")
    
    try:
        # Solicita os dados confirmados
        committed_log = leader.get_committed_log()
        
        if not committed_log:
            print("Nenhum dado confirmado disponível.")
        else:
            print("Dados confirmados consumidos:")
            for entry in committed_log:
                print(f" - {entry['data']} (epoca: {entry['epoca']}, offset: {entry['offset']})")
    
    except Exception as e:
        print(f"Erro ao consumir dados: {e}")

if __name__ == "__main__":
    main()
