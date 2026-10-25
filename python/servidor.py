import json
import os
import re
import time
from datetime import datetime, timezone

import zmq
import mensagem_pb2

NOME = os.environ.get("NOME", "servidor-py")
PASTA_DADOS = "dados"

# letras minusculas, numeros, - e _ (de 1 a 20 caracteres)
NOME_VALIDO = re.compile(r"[a-z0-9_-]{1,20}")

contexto = zmq.Context()
socket_bots = contexto.socket(zmq.REP)

canais = []


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


def criar_mensagem(tipo, **campos):
    return mensagem_pb2.Mensagem(tipo=tipo, remetente=NOME, timestamp=hora_atual(), **campos)


def responder(tipo, erro=None, **campos):
    if erro:
        return criar_mensagem(tipo, status="erro", erro=erro)
    return criar_mensagem(tipo, status="ok", **campos)


def enviar(socket, msg):
    mostrar("ENVIADO", msg)
    socket.send(msg.SerializeToString())


def receber(socket):
    msg = mensagem_pb2.Mensagem.FromString(socket.recv())
    mostrar("RECEBIDO", msg)
    return msg


# ---------------- persistencia (um json por linha) ----------------

def guardar(arquivo, **dados):
    with open(os.path.join(PASTA_DADOS, arquivo), "a") as f:
        f.write(json.dumps(dados) + "\n")


def carregar_canais():
    caminho = os.path.join(PASTA_DADOS, "canais.jsonl")
    if os.path.exists(caminho):
        with open(caminho) as f:
            for linha in f:
                canais.append(json.loads(linha)["canal"])
    print(f"[{NOME}] {len(canais)} canais carregados do disco")


# ---------------- pedidos dos bots ----------------

def fazer_login(pedido):
    if not NOME_VALIDO.fullmatch(pedido.remetente):
        return responder("login", erro="nome invalido (use a-z, 0-9, - ou _ e no maximo 20 letras)")

    guardar("logins.jsonl", usuario=pedido.remetente, timestamp=hora_atual())
    return responder("login")


def criar_canal(pedido):
    nome = pedido.canal
    if not NOME_VALIDO.fullmatch(nome):
        erro = "nome de canal invalido"
    elif nome == "servers":
        erro = "servers e um nome reservado"
    elif nome in canais:
        erro = "canal ja existe"
    else:
        erro = None

    if erro:
        return responder("criar_canal", erro=erro)

    canais.append(nome)
    guardar("canais.jsonl", canal=nome, criado_por=pedido.remetente, timestamp=hora_atual())
    return responder("criar_canal", canal=nome)


def listar_canais(pedido):
    return responder("listar_canais", canais=canais)


TRATADORES = {
    "login": fazer_login,
    "criar_canal": criar_canal,
    "listar_canais": listar_canais,
}


def tratar(pedido):
    tratador = TRATADORES.get(pedido.tipo)
    if tratador is None:
        return responder(pedido.tipo, erro="tipo de mensagem desconhecido")
    return tratador(pedido)


def main():
    os.makedirs(PASTA_DADOS, exist_ok=True)
    carregar_canais()

    socket_bots.connect("tcp://broker:5556")
    print(f"[{NOME}] esperando pedidos do broker...")

    while True:
        pedido = receber(socket_bots)
        enviar(socket_bots, tratar(pedido))


if __name__ == "__main__":
    main()
