import Pyro4

def main():
    leader = Pyro4.Proxy("PYRONAME:Líder-Epoca1")  # Localiza o líder
    while True:
        data = input("Digite uma mensagem para publicar: ")
        response = leader.append_entry(data)
        print(f"Resposta do líder: {response}")

if __name__ == "__main__":
    main()
