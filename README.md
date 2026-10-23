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

## O que tem até agora

- `proto/mensagem.proto`: formato das mensagens (Protobuf)
- `broker/`: broker do request-reply feito em Python
