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

# frases sorteadas para mandar nos canais
FRASES = [
    "oi pessoal",
    "alguem ai?",
    "bom dia!",
    "boa noite galera",
    "testando o canal",
    "o zeromq ta funcionando",
    "mensagem aleatoria",
    "sistemas distribuidos e legal",
    "quem vai na aula amanha?",
    "kkkkkk",
]

contexto = zmq.Context()
req = None
relogio = 0  # relogio logico (lamport)

# socket que recebe as mensagens dos canais em que o bot entrou
sub = contexto.socket(zmq.SUB)
poller = zmq.Poller()
inscrito_em = []


def hora_atual():
    return int(time.time() * 1000)


def hora_legivel(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def mostrar(acao, msg):
    partes = [f"tipo={msg.tipo}", f"remetente={msg.remetente}", f"timestamp={hora_legivel(msg.timestamp)}", f"relogio={msg.relogio}"]
    for campo in ["status", "erro", "canal", "conteudo"]:
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
    global relogio
    relogio += 1  # lamport: soma 1 antes de enviar
    msg = mensagem_pb2.Mensagem(tipo=tipo, remetente=NOME, timestamp=hora_atual(), relogio=relogio, **campos)
    mostrar("ENVIADO", msg)
    req.send(msg.SerializeToString())

    if req.poll(ESPERA_RESPOSTA) == 0:
        print(f"[{NOME}] o servidor nao respondeu, abrindo outro socket")
        req.close()
        conectar()
        return None

    resposta = mensagem_pb2.Mensagem.FromString(req.recv())
    relogio = max(relogio, resposta.relogio)  # lamport: fica com o maior
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


def entrar_em_um_canal(existentes):
    opcoes = [c for c in existentes if c not in inscrito_em]
    if not opcoes:
        return
    canal = random.choice(opcoes)
    sub.setsockopt_string(zmq.SUBSCRIBE, canal)
    inscrito_em.append(canal)
    print(f"[{NOME}] agora estou inscrito em: {', '.join(inscrito_em)}")


def publicar(canal):
    resposta = pedir("publicar", canal=canal, conteudo=random.choice(FRASES))
    if resposta is not None and resposta.status == "erro":
        print(f"[{NOME}] nao publicou no canal {canal}: {resposta.erro}")


# fica ouvindo os canais durante alguns segundos (faz o papel do sleep)
def ouvir_canais(segundos):
    global relogio
    limite = time.monotonic() + segundos
    while True:
        restante = limite - time.monotonic()
        if restante <= 0:
            break
        eventos = dict(poller.poll(int(restante * 1000)))
        if sub not in eventos:
            continue

        topico, dados = sub.recv_multipart()
        # o filtro do zeromq e por prefixo, entao confere o nome inteiro do canal
        if topico.decode() not in inscrito_em:
            continue
        msg = mensagem_pb2.Mensagem.FromString(dados)
        relogio = max(relogio, msg.relogio)
        print(f"[{NOME}] MENSAGEM NO CANAL: canal={msg.canal} | de={msg.remetente} | conteudo={msg.conteudo} | enviada={hora_legivel(msg.timestamp)} | recebida={hora_legivel(hora_atual())} | relogio={msg.relogio}")


def main():
    conectar()
    sub.connect("tcp://proxy:5558")
    poller.register(sub, zmq.POLLIN)
    fazer_login()

    while True:
        existentes = canais_existentes()

        # regra 1: se tiver menos de 5 canais, cria outro
        if len(existentes) < 5:
            criar_canal(existentes)
            existentes = canais_existentes()

        # regra 2: se estiver em menos de 3 canais, entra em mais um
        if len(inscrito_em) < 3:
            entrar_em_um_canal(existentes)

        # regra 3: escolhe um canal e manda 10 mensagens, uma por segundo
        if not existentes:
            ouvir_canais(1)
            continue
        canal = random.choice(existentes)
        for _ in range(10):
            publicar(canal)
            ouvir_canais(1)


if __name__ == "__main__":
    main()
