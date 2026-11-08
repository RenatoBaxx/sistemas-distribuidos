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
PORTA_SERVIDORES = 5560

# letras minusculas, numeros, - e _ (de 1 a 20 caracteres)
NOME_VALIDO = re.compile(r"[a-z0-9_-]{1,20}")

contexto = zmq.Context()
socket_bots = contexto.socket(zmq.REP)
pub = contexto.socket(zmq.PUB)                 # publicacoes nos canais e no topico servers (vai para o proxy)
socket_servidores = contexto.socket(zmq.REP)   # pedidos de outros servidores (eleicao e relogio)
sub_servers = contexto.socket(zmq.SUB)         # avisos de novo coordenador (topico servers)

canais = []
relogio = 0   # relogio logico (lamport)
desvio = 0    # quanto o relogio fisico deste servidor esta ajustado (ms)
meu_rank = 0
coordenador = ""
atendidos = 0
diferencas = {}  # berkeley: nome do servidor -> diferenca para o relogio do coordenador


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
    resposta = perguntar(REFERENCIA, criar_mensagem("heartbeat"))
    if resposta is None:
        print(f"[{NOME}] a referencia nao respondeu o heartbeat")
    elif resposta.status == "erro":
        pegar_rank()  # a referencia reiniciou e esqueceu este servidor


# ---------------- eleicao (algoritmo do valentao / bully) ----------------

def endereco_de(nome_servidor):
    return f"tcp://{nome_servidor}:{PORTA_SERVIDORES}"


def fazer_eleicao():
    global coordenador
    print(f"[{NOME}] comecando eleicao")
    maiores = [s for s in lista_de_servidores() if s.rank > meu_rank]
    respostas = [perguntar(endereco_de(s.nome), criar_mensagem("eleicao")) for s in maiores]

    if any(r is not None and r.status == "ok" for r in respostas):
        # um servidor com rank maior vai assumir e avisar no topico servers
        coordenador = ""
        print(f"[{NOME}] tem servidor com rank maior vivo, esperando o aviso do novo coordenador")
        return

    coordenador = NOME
    diferencas.clear()
    aviso = criar_mensagem("coordenador", conteudo=NOME, rank=meu_rank)
    marcar_envio(aviso)
    mostrar("PUBLICADO", aviso)
    pub.send_multipart([b"servers", aviso.SerializeToString()])
    print(f"[{NOME}] ganhei a eleicao, agora eu sou o coordenador")


def receber_aviso():
    global coordenador
    topico, dados = sub_servers.recv_multipart()
    aviso = ler(dados)
    if topico != b"servers" or aviso.tipo != "coordenador":
        return

    coordenador = aviso.conteudo
    print(f"[{NOME}] o coordenador agora e {coordenador}")
    # bully: se quem ganhou tem rank menor que o meu, eu preciso assumir
    if aviso.rank < meu_rank:
        fazer_eleicao()


# berkeley (parte do coordenador): guarda a diferenca de quem pediu, tira a media de todas
# (o coordenador entra como 0), anda o proprio relogio ate a media e devolve a hora
def responder_relogio(pedido):
    global desvio
    if coordenador != NOME:
        return responder("relogio", erro="nao sou o coordenador")

    diferencas[pedido.remetente] = pedido.hora - hora_atual()
    media = sum(diferencas.values()) // (len(diferencas) + 1)
    desvio += media
    for nome in diferencas:
        diferencas[nome] -= media
    diferencas[pedido.remetente] = 0  # quem pediu vai usar a hora da resposta
    return responder("relogio", hora=hora_atual())


# berkeley (parte de quem pede): manda a sua hora para o coordenador e usa a hora da resposta.
# se o coordenador nao responder, considera que ele caiu e faz uma eleicao
def sincronizar_relogio():
    global desvio, coordenador
    if coordenador == "":
        fazer_eleicao()
        return
    if coordenador == NOME:
        return

    resposta = perguntar(endereco_de(coordenador), criar_mensagem("relogio", hora=hora_atual()))
    if resposta is None or resposta.status != "ok":
        print(f"[{NOME}] o coordenador {coordenador} nao respondeu, comecando nova eleicao")
        coordenador = ""
        fazer_eleicao()
        return

    diferenca = resposta.hora - hora_atual()
    desvio += diferenca
    print(f"[{NOME}] relogio fisico ajustado em {diferenca} ms pelo coordenador {coordenador}")


# pedidos que chegam de outros servidores pela porta 5560
def atender_servidor():
    pedido = ler(socket_servidores.recv())
    if pedido.tipo == "eleicao":
        enviar(socket_servidores, responder("eleicao"))
        fazer_eleicao()  # quem pediu tem rank menor, entao a eleicao continua daqui
    elif pedido.tipo == "relogio":
        enviar(socket_servidores, responder_relogio(pedido))
    else:
        enviar(socket_servidores, responder(pedido.tipo, erro="tipo de mensagem desconhecido"))


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


def atender_bot():
    global atendidos
    pedido = ler(socket_bots.recv())
    enviar(socket_bots, tratar(pedido))

    atendidos += 1
    if atendidos % 10 == 0:
        mandar_heartbeat()
    if atendidos % 15 == 0:
        sincronizar_relogio()


def main():
    global desvio
    os.makedirs(PASTA_DADOS, exist_ok=True)
    carregar_canais()

    # todos os containers usam o relogio do mesmo computador,
    # entao o desvio aleatorio simula servidores com relogios diferentes
    desvio = random.randint(-3000, 3000)
    print(f"[{NOME}] relogio fisico comecando com {desvio} ms de desvio")

    pub.connect("tcp://proxy:5557")
    sub_servers.connect("tcp://proxy:5558")
    sub_servers.setsockopt_string(zmq.SUBSCRIBE, "servers")
    socket_servidores.bind(f"tcp://*:{PORTA_SERVIDORES}")

    pegar_rank()
    servidores = lista_de_servidores()
    print(f"[{NOME}] servidores disponiveis: " + ", ".join(f"{s.nome} (rank {s.rank})" for s in servidores))

    time.sleep(1)  # da tempo do SUB do topico servers conectar no proxy
    fazer_eleicao()

    socket_bots.connect("tcp://broker:5556")
    print(f"[{NOME}] esperando pedidos do broker...")

    poller = zmq.Poller()
    poller.register(socket_bots, zmq.POLLIN)
    poller.register(socket_servidores, zmq.POLLIN)
    poller.register(sub_servers, zmq.POLLIN)

    while True:
        prontos = dict(poller.poll())
        if socket_servidores in prontos:
            atender_servidor()
        if sub_servers in prontos:
            receber_aviso()
        if socket_bots in prontos:
            atender_bot()


if __name__ == "__main__":
    main()
