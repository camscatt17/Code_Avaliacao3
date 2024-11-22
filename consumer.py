import Pyro4

def main():
    broker_id = input("Digite o ID do broker para consumir dados: ")
    broker = Pyro4.Proxy(f"PYRONAME:broker.{broker_id}")  # Localiza o broker
    while True:
        input("Pressione Enter para consumir a próxima mensagem...")
        data = broker.consume_data()
        print(f"Mensagem consumida: {data}")

if __name__ == "__main__":
    main()
