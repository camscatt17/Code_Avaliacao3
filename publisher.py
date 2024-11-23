import Pyro4

def main():
    # Conecta-se ao líder via serviço de nomes
    leader = Pyro4.Proxy("PYRONAME:Líder-Epoca1")

    while True:
        data = input("Insira os dados para publicar (ou 'exit' para sair): ")
        if data.lower() == "exit":
            break
        
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

if __name__ == "__main__":
    main()
