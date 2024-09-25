import json


def open_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Arquivo {filepath} não encontrado.")
    except json.JSONDecodeError:
        print("Erro ao decodificar o arquivo JSON.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

