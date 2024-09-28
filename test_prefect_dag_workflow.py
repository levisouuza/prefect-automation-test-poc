from prefect import flow, task
from prefect.states import Failed


@task
def tarefa_1():
    print("Executando tarefa 1")
    return "resultado de tarefa 1"


@task
def tarefa_2(dep):
    print(f"Executando tarefa 2 após {dep}")
    return "resultado de tarefa 2"


@task
def tarefa_3(dep):
    print(f"Executando tarefa 3 após {dep}")
    return "resultado de tarefa 3"


@task
def tarefa_4(dep_2, dep_3):
    if dep_2 and dep_3:
        print(f"Executando tarefa 4 após {dep_2} e {dep_3}")
    else:
        raise Failed("Tarefa 2 ou Tarefa 3 falhou.")  # Levanta uma exceção de falha específica do Prefect


@flow
def meu_flow():
    # Tarefa 1 inicia o fluxo
    resultado_1 = tarefa_1()

    # Tarefa 2 e Tarefa 3 são executadas em paralelo
    resultado_2 = tarefa_2.submit(resultado_1)  # Executada após tarefa_1
    resultado_3 = tarefa_3.submit(resultado_1)  # Executada após tarefa_1

    # Tarefa 4 só executa após tarefa 2 e 3 terminarem
    tarefa_4.submit(resultado_2, resultado_3)


meu_flow()
