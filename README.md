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

O broker foi feito em Python (`broker/broker.py`). Qualquer bot conversa com qualquer
servidor, porque o broker não sabe a linguagem de ninguém, ele só repassa as mensagens.

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
| tipo | string | `login`, `criar_canal`, `listar_canais` |
| remetente | string | nome de quem enviou (bot ou servidor) |
| timestamp | int64 | hora do envio em milissegundos (unix) |
| relogio | int64 | relógio lógico (vai ser usado na parte 3) |
| status | string | `ok` ou `erro` nas respostas |
| erro | string | descrição do erro |
| canal | string | nome do canal |
| conteudo | string | texto da mensagem (vai ser usado na parte 2) |
| canais | lista de string | lista de canais |

### Persistência

Cada servidor salva os dados na sua própria pasta (`dados/<nome do servidor>/`), que é um
volume do Docker, então nada se perde quando os containers são reiniciados. Os arquivos
têm um registro por linha em JSON (JSON Lines):

- `logins.jsonl`: todos os logins (usuário e timestamp)
- `canais.jsonl`: canais criados (nome, quem criou e timestamp)

Quando o servidor inicia ele lê o `canais.jsonl` para recuperar os canais.

> O JSON é usado só para guardar os dados no disco. As mensagens que passam pela rede são
> sempre Protobuf (binário).

## Como funciona

```
bots (REQ) ---> broker (ROUTER 5555 | DEALER 5556) ---> servidores (REP)
```

Os bots se conectam na porta 5555 do broker e os servidores na 5556. O broker distribui os
pedidos entre os 4 servidores usando round-robin.

### Login

O bot manda `login` com o seu nome no campo `remetente` e o servidor responde `ok` ou
`erro`. Se der erro, o bot tenta de novo depois de 1 segundo.

### Canais

- `criar_canal`: o bot manda o nome no campo `canal` e o servidor responde `ok` ou `erro`.
- `listar_canais`: o servidor responde com a lista no campo `canais`.

O bot lista os canais a cada 3 segundos e, se existirem menos de 5, cria um novo com um
nome sorteado de uma lista fixa.

### Erros tratados

| Pedido | Quando dá erro |
|---|---|
| login | nome vazio, com mais de 20 caracteres ou com caracteres fora de `a-z`, `0-9`, `-` e `_` |
| criar_canal | nome inválido (mesma regra do login), canal que já existe ou o nome `servers` (reservado para a parte 4) |
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
csharp/Comum/           código usado pelo servidor e pelo bot em C# (Mensagem, log e envio)
csharp/Servidor/        servidor em C#
csharp/Cliente/         bot em C#
csharp/Dockerfile       imagem dos programas em C#
python/servidor.py      servidor em Python
python/cliente.py       bot em Python
python/Dockerfile       imagem dos programas em Python
docker-compose.yaml     sobe o broker, 4 servidores e 4 bots
```
