import time
from datetime import datetime, timezone

import zmq
import mensagem_pb2

NOME = "referencia"
TEMPO_SEM_HEARTBEAT = 30  # segundos sem heartbeat para o servidor sair da lista

contexto = zmq.Context()

ranks = {}           # nome do servidor -> rank (nao apaga, se o servidor voltar fica com o mesmo rank)
ultimo_contato = {}  # nome do servidor -> time.time() do ultimo contato (so os disponiveis)


def hora_atual():
    return int(time.time() * 1000)


def hora_legivel(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def mostrar(acao, msg):
    partes = [f"tipo={msg.tipo}", f"remetente={msg.remetente}", f"timestamp={hora_legivel(msg.timestamp)}"]
    for campo in ["status", "erro", "rank"]:
        valor = getattr(msg, campo)
        if valor:
            partes.append(f"{campo}={valor}")
    if msg.servidores:
        partes.append("servidores=" + ",".join(f"{s.nome}(rank {s.rank})" for s in msg.servidores))
    if msg.hora:
        partes.append(f"hora={hora_legivel(msg.hora)}")
    print(f"[{NOME}] {acao}: " + " | ".join(partes))


def responder(tipo, erro=None, **campos):
    if erro:
        return mensagem_pb2.Mensagem(tipo=tipo, remetente=NOME, timestamp=hora_atual(), status="erro", erro=erro)
    return mensagem_pb2.Mensagem(tipo=tipo, remetente=NOME, timestamp=hora_atual(), status="ok", **campos)


def enviar(socket, msg):
    mostrar("ENVIADO", msg)
    socket.send(msg.SerializeToString())


def receber(socket):
    msg = mensagem_pb2.Mensagem.FromString(socket.recv())
    mostrar("RECEBIDO", msg)
    return msg


def dar_rank(pedido):
    nome = pedido.remetente
    if nome not in ranks:
        ranks[nome] = len(ranks) + 1
        print(f"[{NOME}] novo servidor: {nome} ficou com rank {ranks[nome]}")
    ultimo_contato[nome] = time.time()
    return responder("rank", rank=ranks[nome])


def listar_servidores(pedido):
    resposta = responder("listar_servidores")
    for nome in sorted(ultimo_contato, key=lambda n: ranks[n]):
        resposta.servidores.add(nome=nome, rank=ranks[nome])
    return resposta


def receber_heartbeat(pedido):
    nome = pedido.remetente
    if nome not in ranks:
        return responder("heartbeat", erro="servidor sem rank, peca o rank primeiro")
    ultimo_contato[nome] = time.time()
    # aproveita a resposta do heartbeat para mandar a hora certa
    return responder("heartbeat", hora=hora_atual())


def remover_inativos():
    agora = time.time()
    for nome in list(ultimo_contato):
        if agora - ultimo_contato[nome] > TEMPO_SEM_HEARTBEAT:
            del ultimo_contato[nome]
            print(f"[{NOME}] {nome} ficou sem heartbeat e saiu da lista de servidores")


TRATADORES = {
    "rank": dar_rank,
    "listar_servidores": listar_servidores,
    "heartbeat": receber_heartbeat,
}


def main():
    socket = contexto.socket(zmq.REP)
    socket.bind("tcp://*:5559")
    print(f"[{NOME}] servico de referencia rodando na porta 5559")

    while True:
        # espera no maximo 1 segundo por pedido para dar tempo de conferir os heartbeats
        if socket.poll(1000):
            pedido = receber(socket)
            tratador = TRATADORES.get(pedido.tipo)
            if tratador is None:
                enviar(socket, responder(pedido.tipo, erro="tipo de mensagem desconhecido"))
            else:
                enviar(socket, tratador(pedido))
        remover_inativos()


if __name__ == "__main__":
    main()
