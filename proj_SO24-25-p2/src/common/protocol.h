#ifndef COMMON_PROTOCOL_H
#define COMMON_PROTOCOL_H

// Opcodes for client-server communication
// estes opcodes sao usados num switch case para determinar o que fazer com a
// mensagem recebida no server usam estes opcodes tambem nos clientes quando
// enviam mensagens para o server
enum {
  OP_CODE_CONNECT = 'C',
  OP_CODE_DISCONNECT = 'D',
  OP_CODE_SUBSCRIBE = 'S',
  OP_CODE_UNSUBSCRIBE = 'U',
};

#endif // COMMON_PROTOCOL_H
