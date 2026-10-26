import os
import random
import time
from datetime import datetime, timezone

import zmq
import mensagem_pb2

NOME = os.environ.get("NOME", "bot-py")
BROKER = "tcp://broker:5555"
ESPERA_RESPOSTA = 5000  # ms

# canais que o bot pode criar
SUGESTOES_DE_CANAL = ["geral", "jogos", "musica", "filmes", "esportes", "noticias", "tecnologia", "memes"]

contexto = zmq.Context()
req = None


def hora_atual():
    return int(time.time() * 1000)


def hora_legivel(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def mostrar(acao, msg):
    partes = [f"tipo={msg.tipo}", f"remetente={msg.remetente}", f"timestamp={hora_legivel(msg.timestamp)}"]
    for campo in ["status", "erro", "canal"]:
        valor = getattr(msg, campo)
        if valor:
            partes.append(f"{campo}={valor}")
    if msg.canais:
        partes.append("canais=" + ",".join(msg.canais))
    print(f"[{NOME}] {acao}: " + " | ".join(partes))


def conectar():
    global req
    req = contexto.socket(zmq.REQ)
    req.setsockopt(zmq.LINGER, 0)
    req.connect(BROKER)


# o REQ nao deixa mandar outro pedido sem receber a resposta,
# entao se o servidor nao responder a tempo o socket e trocado por um novo
def pedir(tipo, **campos):
    msg = mensagem_pb2.Mensagem(tipo=tipo, remetente=NOME, timestamp=hora_atual(), **campos)
    mostrar("ENVIADO", msg)
    req.send(msg.SerializeToString())

    if req.poll(ESPERA_RESPOSTA) == 0:
        print(f"[{NOME}] o servidor nao respondeu, abrindo outro socket")
        req.close()
        conectar()
        return None

    resposta = mensagem_pb2.Mensagem.FromString(req.recv())
    mostrar("RECEBIDO", resposta)
    return resposta


def fazer_login():
    resposta = pedir("login")
    while resposta is None or resposta.status != "ok":
        print(f"[{NOME}] login nao deu certo, tentando de novo em 1 segundo")
        time.sleep(1)
        resposta = pedir("login")
    print(f"[{NOME}] login ok")


def canais_existentes():
    resposta = pedir("listar_canais")
    if resposta is None or resposta.status != "ok":
        return []
    return list(resposta.canais)


def criar_canal(existentes):
    livres = [c for c in SUGESTOES_DE_CANAL if c not in existentes]
    if not livres:
        return
    resposta = pedir("criar_canal", canal=random.choice(livres))
    if resposta is not None and resposta.status == "erro":
        print(f"[{NOME}] nao criou o canal: {resposta.erro}")


def main():
    conectar()
    fazer_login()

    while True:
        existentes = canais_existentes()
        if len(existentes) < 5:
            criar_canal(existentes)
        time.sleep(3)


if __name__ == "__main__":
    main()
