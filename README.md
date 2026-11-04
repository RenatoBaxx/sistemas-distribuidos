# Sistema de troca de mensagens (estilo BBS/IRC)

Projeto da disciplina CC7261 - Sistemas Distribuídos (FEI).

A ideia do projeto é fazer uma versão simples de um sistema de troca de mensagens
parecido com o IRC, onde bots fazem login, criam canais e trocam mensagens entre si.
Toda a comunicação é feita com ZeroMQ e tudo roda em containers com Docker Compose.

## Integrantes

- Integrante 1 - C#
- Integrante 2 - Python

## Como rodar

```
docker compose up --build
```

Não precisa digitar nada, os bots fazem tudo sozinhos. Para parar é só dar `Ctrl+C`
e depois `docker compose down`.

## Escolhas do grupo

### Linguagens

Cada integrante fez um bot (cliente) e um servidor na sua linguagem:

| Linguagem | Biblioteca do ZeroMQ | Código |
|---|---|---|
| C# (.NET 10) | NetMQ | `csharp/Servidor`, `csharp/Cliente` e `csharp/Comum` |
| Python 3.12 | pyzmq | `python/servidor.py` e `python/cliente.py` |

O broker e o serviço de referência foram feitos em Python (`broker/` e `referencia/`) e o
proxy do Pub/Sub em C# (`proxy/`). Qualquer bot conversa com qualquer servidor, porque o
broker e o proxy não sabem a linguagem de ninguém, eles só repassam as mensagens.

### Serialização: Protobuf

As mensagens são serializadas em binário com Protobuf. Escolhemos o Protobuf porque um
único arquivo (`proto/mensagem.proto`) gera as classes em C# e em Python, assim os dois
lados sempre usam exatamente o mesmo formato. As classes são geradas no build do Docker
(`Grpc.Tools` no C# e `grpcio-tools` no Python).

### Formato da mensagem

Usamos uma mensagem só (`Mensagem`) para tudo e o campo `tipo` diz o que ela é.
A resposta sempre usa o mesmo `tipo` do pedido.

| Campo | Tipo | Para que serve |
|---|---|---|
| tipo | string | `login`, `criar_canal`, `listar_canais`, `publicar`, `publicacao`, `rank`, `listar_servidores`, `heartbeat` |
| remetente | string | nome de quem enviou (bot ou servidor) |
| timestamp | int64 | hora do envio em milissegundos (unix) |
| relogio | int64 | relógio lógico (Lamport) de quem enviou |
| status | string | `ok` ou `erro` nas respostas |
| erro | string | descrição do erro |
| canal | string | nome do canal |
| conteudo | string | texto da mensagem |
| canais | lista de string | lista de canais |
| rank | int32 | rank do servidor (dado pela referência) |
| servidores | lista de `InfoServidor` (nome e rank) | servidores disponíveis |
| hora | int64 | hora usada para acertar o relógio físico |

Os campos `rank`, `servidores` e `hora` foram adicionados na parte 3. Como o Protobuf
aceita campos novos sem quebrar o que já existia, nada das partes anteriores mudou.

### Persistência

Cada servidor salva os dados na sua própria pasta (`dados/<nome do servidor>/`), que é um
volume do Docker, então nada se perde quando os containers são reiniciados. Os arquivos
têm um registro por linha em JSON (JSON Lines):

- `logins.jsonl`: todos os logins (usuário e timestamp)
- `canais.jsonl`: canais criados (nome, quem criou e timestamp)
- `mensagens.jsonl`: mensagens publicadas (canal, usuário, texto, timestamp e relógio lógico do envio)

Quando o servidor inicia ele lê o `canais.jsonl` para recuperar os canais.

> O JSON é usado só para guardar os dados no disco. As mensagens que passam pela rede são
> sempre Protobuf (binário).

## Como funciona

```
bots (REQ) ---> broker (ROUTER 5555 | DEALER 5556) ---> servidores (REP)
servidores (PUB) ---> proxy (XSUB 5557 | XPUB 5558) ---> bots (SUB)
servidores (REQ) ---> referencia (REP 5559)
```

Os bots se conectam na porta 5555 do broker e os servidores na 5556. O broker distribui os
pedidos entre os 4 servidores usando round-robin.

Para as mensagens dos canais existe um proxy separado: os servidores publicam na porta
5557 e os bots assinam na porta 5558.

### Login

O bot manda `login` com o seu nome no campo `remetente` e o servidor responde `ok` ou
`erro`. Se der erro, o bot tenta de novo depois de 1 segundo.

### Canais

- `criar_canal`: o bot manda o nome no campo `canal` e o servidor responde `ok` ou `erro`.
- `listar_canais`: o servidor responde com a lista no campo `canais`.

### Publicação nos canais (Pub/Sub)

O nome do canal é o tópico do Pub/Sub. Para publicar, o bot faz um pedido `publicar` ao
servidor (pelo broker) com o canal e o texto. O servidor publica no proxy e responde `ok`
ou `erro` para o bot:

```
bot      -> servidor : REQ publicar (canal, conteudo)
servidor -> proxy    : PUB [canal] publicacao
servidor -> bot      : REP ok/erro
```

A publicação tem 2 partes (frames): a primeira é o nome do canal, porque o ZeroMQ usa o
começo da mensagem para filtrar o tópico, e a segunda é a `Mensagem` em Protobuf com
`tipo = publicacao`. Na publicação o `remetente` é o bot que escreveu a mensagem, para
quem recebe saber quem mandou.

O servidor não confere se o canal existe na lista dele, só se o nome é válido, porque com
o round-robin cada servidor só conhece uma parte dos canais (ver a observação no final).

Quando chega uma mensagem de um canal em que o bot está inscrito, ele mostra o canal, quem
mandou, o texto, a hora do envio e a hora do recebimento. Cada bot mostra do seu jeito:

```
[bot-py-1] MENSAGEM NO CANAL: canal=geral | de=bot-cs-2 | conteudo=bom dia! | enviada=00:57:36.461 | recebida=00:57:36.478
[bot-cs-1] #geral <bot-py-2> bom dia!  (enviada 00:42:32.577, recebida 00:42:32.578)
```

### Roteiro dos bots

Depois do login, o bot fica em um loop infinito fazendo:

1. lista os canais e, se existirem menos de 5, cria um novo;
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um (sorteado da lista);
3. escolhe um canal da lista e manda 10 mensagens aleatórias, uma a cada 1 segundo.

Os passos 1 e 2 são feitos a cada volta do loop (e não só uma vez), assim o bot vai se
inscrevendo aos poucos até chegar em 3 canais. Enquanto espera o 1 segundo entre as
mensagens, o bot fica lendo o socket SUB e mostrando o que chegou, sem precisar de thread.

### Relógio lógico (Lamport)

Todos os bots, servidores e o serviço de referência têm um contador que começa em 0:

1. antes de enviar qualquer mensagem o contador aumenta 1 e vai no campo `relogio`;
2. quando recebe uma mensagem, o processo fica com o maior valor entre o seu contador e o
   `relogio` que veio na mensagem.

Isso vale para todas as mensagens: pedidos, respostas, publicações nos canais e as
mensagens trocadas com a referência. O broker e o proxy só repassam bytes, então não têm
relógio.

### Serviço de referência

Processo novo (`referencia/`, porta 5559) que só conversa com os servidores:

| Pedido | O que faz |
|---|---|
| `rank` | o servidor manda o seu nome e recebe o rank. O rank é a ordem de chegada (1, 2, 3...) e o mesmo nome sempre recebe o mesmo rank |
| `listar_servidores` | devolve nome e rank de todos os servidores disponíveis |
| `heartbeat` | avisa que o servidor continua vivo; a resposta traz a hora certa no campo `hora` |

Cada servidor pede o rank e a lista quando inicia e manda um heartbeat a cada 10 pedidos de
bots atendidos. Se um servidor ficar 30 segundos sem mandar heartbeat ele sai da lista de
disponíveis (volta quando mandar outro). Se a referência reiniciar e não conhecer mais o
servidor, ela responde `erro` no heartbeat e o servidor pede o rank de novo.

### Relógio físico

Como todos os containers usam o relógio do mesmo computador, os relógios iriam ficar
sempre iguais. Para dar para ver a sincronização funcionando, cada servidor começa com um
erro aleatório de até 3 segundos para mais ou para menos. A cada heartbeat o servidor
compara a sua hora com a hora que veio na resposta e corrige a diferença:

```
[servidor-py-1] relogio fisico comecando com -2049 ms de desvio
[servidor-py-1] relogio fisico ajustado em 2048 ms com a hora da referencia
```

Toda hora que aparece nas mensagens dos servidores (`timestamp`) já usa o relógio ajustado.

### Erros tratados

| Pedido | Quando dá erro |
|---|---|
| login | nome vazio, com mais de 20 caracteres ou com caracteres fora de `a-z`, `0-9`, `-` e `_` |
| criar_canal | nome inválido (mesma regra do login), canal que já existe ou o nome `servers` (reservado para a parte 4) |
| publicar | nome de canal inválido, canal `servers` ou mensagem vazia |
| outro tipo | `tipo de mensagem desconhecido` |

Login repetido é aceito, só fica registrado de novo no arquivo.

Os bots esperam a resposta por no máximo 5 segundos. Assim, se um servidor cair no meio
de um pedido, o bot não fica travado esperando para sempre (no C# cada pedido usa um
socket REQ novo e no Python o socket REQ é recriado quando estoura o tempo).

### Observação sobre o round-robin

Como cada pedido pode cair em um servidor diferente e cada servidor tem os seus próprios
dados, a lista de canais muda dependendo de qual servidor respondeu. Por exemplo, um bot
pode ver numa lista que o canal `jogos` não existe e, ao tentar criar, receber
`canal ja existe` de outro servidor. Isso é esperado nesta parte e vai ser resolvido na
parte 5 (replicação).

## Estrutura de pastas

```
proto/mensagem.proto    formato das mensagens
broker/                 broker req/rep (Python)
proxy/                  proxy pub/sub (C#)
referencia/             serviço de referência (Python)
csharp/Comum/           código usado pelo servidor e pelo bot em C# (Mensagem, log e envio)
csharp/Servidor/        servidor em C#
csharp/Cliente/         bot em C#
csharp/Dockerfile       imagem dos programas em C#
python/servidor.py      servidor em Python
python/cliente.py       bot em Python
python/Dockerfile       imagem dos programas em Python
docker-compose.yaml     sobe o broker, o proxy, a referência, 4 servidores e 4 bots
```
