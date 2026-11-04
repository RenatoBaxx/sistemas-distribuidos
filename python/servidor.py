import json
import os
import random
import re
import time
from datetime import datetime, timezone

import zmq
import mensagem_pb2

NOME = os.environ.get("NOME", "servidor-py")
PASTA_DADOS = "dados"
REFERENCIA = "tcp://referencia:5559"

# letras minusculas, numeros, - e _ (de 1 a 20 caracteres)
NOME_VALIDO = re.compile(r"[a-z0-9_-]{1,20}")

contexto = zmq.Context()
socket_bots = contexto.socket(zmq.REP)
pub = contexto.socket(zmq.PUB)  # publicacoes nos canais (vai para o proxy)

canais = []
relogio = 0   # relogio logico (lamport)
desvio = 0    # quanto o relogio fisico deste servidor esta ajustado (ms)
meu_rank = 0


def hora_atual():
    return int(time.time() * 1000) + desvio


def hora_legivel(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def mostrar(acao, msg):
    partes = [f"tipo={msg.tipo}", f"remetente={msg.remetente}", f"timestamp={hora_legivel(msg.timestamp)}", f"relogio={msg.relogio}"]
    for campo in ["status", "erro", "canal", "conteudo", "rank"]:
        valor = getattr(msg, campo)
        if valor:
            partes.append(f"{campo}={valor}")
    if msg.canais:
        partes.append("canais=" + ",".join(msg.canais))
    if msg.servidores:
        partes.append("servidores=" + ",".join(f"{s.nome}(rank {s.rank})" for s in msg.servidores))
    if msg.hora:
        partes.append(f"hora={hora_legivel(msg.hora)}")
    print(f"[{NOME}] {acao}: " + " | ".join(partes))


def criar_mensagem(tipo, **campos):
    return mensagem_pb2.Mensagem(tipo=tipo, remetente=NOME, timestamp=hora_atual(), **campos)


def responder(tipo, erro=None, **campos):
    if erro:
        return criar_mensagem(tipo, status="erro", erro=erro)
    return criar_mensagem(tipo, status="ok", **campos)


# lamport: soma 1 no relogio antes de cada envio e manda o valor junto
def marcar_envio(msg):
    global relogio
    relogio += 1
    msg.relogio = relogio


def enviar(socket, msg):
    marcar_envio(msg)
    mostrar("ENVIADO", msg)
    socket.send(msg.SerializeToString())


# lamport: ao receber fica com o maior valor entre o meu relogio e o da mensagem
def ler(dados):
    global relogio
    msg = mensagem_pb2.Mensagem.FromString(dados)
    relogio = max(relogio, msg.relogio)
    mostrar("RECEBIDO", msg)
    return msg


# faz um pedido com um socket REQ novo; devolve None se nao vier resposta a tempo
def perguntar(endereco, msg, espera=2000):
    sock = contexto.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(endereco)
    enviar(sock, msg)
    resposta = None
    if sock.poll(espera):
        resposta = ler(sock.recv())
    sock.close()
    return resposta


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


# ---------------- servico de referencia ----------------

def pegar_rank():
    global meu_rank
    resposta = perguntar(REFERENCIA, criar_mensagem("rank"))
    while resposta is None or resposta.status != "ok":
        print(f"[{NOME}] a referencia nao respondeu, tentando de novo...")
        time.sleep(1)
        resposta = perguntar(REFERENCIA, criar_mensagem("rank"))
    meu_rank = resposta.rank
    print(f"[{NOME}] meu rank e {meu_rank}")


def lista_de_servidores():
    resposta = perguntar(REFERENCIA, criar_mensagem("listar_servidores"))
    if resposta is None or resposta.status != "ok":
        return []
    return list(resposta.servidores)


def mandar_heartbeat():
    global desvio
    resposta = perguntar(REFERENCIA, criar_mensagem("heartbeat"))
    if resposta is None:
        print(f"[{NOME}] a referencia nao respondeu o heartbeat")
    elif resposta.status == "erro":
        pegar_rank()  # a referencia reiniciou e esqueceu este servidor
    else:
        # a resposta do heartbeat traz a hora certa da referencia
        diferenca = resposta.hora - hora_atual()
        desvio += diferenca
        print(f"[{NOME}] relogio fisico ajustado em {diferenca} ms com a hora da referencia")


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


# nao confere se o canal existe aqui porque, com o round-robin, cada servidor so conhece parte dos canais
def publicar(pedido):
    if not NOME_VALIDO.fullmatch(pedido.canal) or pedido.canal == "servers":
        return responder("publicar", erro="canal invalido")
    if not pedido.conteudo.strip():
        return responder("publicar", erro="mensagem vazia")

    # o remetente da publicacao e o bot que escreveu a mensagem
    publicacao = mensagem_pb2.Mensagem(
        tipo="publicacao",
        remetente=pedido.remetente,
        timestamp=hora_atual(),
        canal=pedido.canal,
        conteudo=pedido.conteudo,
    )
    marcar_envio(publicacao)
    mostrar("PUBLICADO", publicacao)
    pub.send_multipart([pedido.canal.encode(), publicacao.SerializeToString()])

    guardar("mensagens.jsonl", canal=pedido.canal, usuario=pedido.remetente, conteudo=pedido.conteudo,
            timestamp=pedido.timestamp, relogio=pedido.relogio)
    return responder("publicar", canal=pedido.canal)


TRATADORES = {
    "login": fazer_login,
    "criar_canal": criar_canal,
    "listar_canais": listar_canais,
    "publicar": publicar,
}


def tratar(pedido):
    tratador = TRATADORES.get(pedido.tipo)
    if tratador is None:
        return responder(pedido.tipo, erro="tipo de mensagem desconhecido")
    return tratador(pedido)


def main():
    global desvio
    os.makedirs(PASTA_DADOS, exist_ok=True)
    carregar_canais()

    # todos os containers usam o relogio do mesmo computador,
    # entao o desvio aleatorio simula servidores com relogios diferentes
    desvio = random.randint(-3000, 3000)
    print(f"[{NOME}] relogio fisico comecando com {desvio} ms de desvio")

    pub.connect("tcp://proxy:5557")
    pegar_rank()
    servidores = lista_de_servidores()
    print(f"[{NOME}] servidores disponiveis: " + ", ".join(f"{s.nome} (rank {s.rank})" for s in servidores))

    socket_bots.connect("tcp://broker:5556")
    print(f"[{NOME}] esperando pedidos do broker...")

    atendidos = 0
    while True:
        pedido = ler(socket_bots.recv())
        enviar(socket_bots, tratar(pedido))

        atendidos += 1
        if atendidos % 10 == 0:
            mandar_heartbeat()


if __name__ == "__main__":
    main()
