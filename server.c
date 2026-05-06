/*
 * server.c - Servidor del servicio de mensajeria distribuido
 * Sistemas Distribuidos - UC3M - Curso 2025-2026
 *
 * Servidor concurrente multihilo que gestiona:
 *   - Registro/baja de usuarios
 *   - Conexion/desconexion de usuarios
 *   - Envio y almacenamiento de mensajes entre usuarios
 *   - Solicitud de usuarios conectados
 *   - (Parte 2) Mensajes con fichero adjunto
 *   - (Parte 2) Logging via RPC
 *
 * Uso: ./server -p <puerto>
 */

#include "rpc_client.h" /* logging via RPC (Parte 2) */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ===================== CONSTANTES ===================== */

#define MAX_USERS 128
#define MAX_MESSAGES 512
#define MAX_NAME 256
#define MAX_MSG_TEXT 256
#define MAX_FILENAME 256
#define MAX_IP 64
#define BACKLOG 16

/* Operaciones del protocolo cliente->servidor */
#define OP_REGISTER "REGISTER"
#define OP_UNREGISTER "UNREGISTER"
#define OP_CONNECT "CONNECT"
#define OP_DISCONNECT "DISCONNECT"
#define OP_SEND "SEND"
#define OP_SENDATTACH "SENDATTACH"
#define OP_USERS "USERS"

/* Operaciones del protocolo servidor->cliente (hilo receptor) */
#define OP_SEND_MESSAGE "SEND_MESSAGE"
#define OP_SEND_MESS_ACK "SEND_MESS_ACK"
#define OP_SEND_MESSAGE_ATTACH "SEND_MESSAGE_ATTACH"
#define OP_SEND_MESS_ATTACH_ACK "SEND_MESS_ATTACH_ACK"

/* Codigos de respuesta enviados al cliente (1 byte) */
#define RC_OK 0
#define RC_USER_ERROR 1
#define RC_ERROR 2
#define RC_ALREADY 2  /* alias para "ya conectado" en CONNECT */
#define RC_NOT_CONN 2 /* alias para "no conectado" en DISCONNECT */
#define RC_FAIL 3

/* ===================== ESTRUCTURAS ===================== */

/*
 * Mensaje pendiente de entrega a un usuario.
 * Forma una lista enlazada por usuario destinatario.
 */
typedef struct Message {
  unsigned int id;             /* identificador numerico del mensaje */
  char sender[MAX_NAME];       /* usuario remitente */
  char text[MAX_MSG_TEXT];     /* contenido del mensaje */
  char filename[MAX_FILENAME]; /* nombre fichero adjunto (Parte 2), "" si
                                  ninguno */
  struct Message *next;        /* siguiente mensaje pendiente */
} Message;

/*
 * Entrada de usuario en la tabla global.
 * Estado: 0=desconectado, 1=conectado.
 */
typedef struct {
  int active;               /* 1 si la entrada esta en uso */
  char name[MAX_NAME];      /* nombre de usuario */
  int connected;            /* 0=desconectado, 1=conectado */
  char ip[MAX_IP];          /* IP del hilo receptor del cliente */
  int port;                 /* puerto del hilo receptor del cliente */
  unsigned int last_msg_id; /* ultimo id de mensaje asignado */
  Message *pending;         /* lista de mensajes pendientes */
} User;

/* ===================== VARIABLES GLOBALES ===================== */

static User users[MAX_USERS]; /* tabla de usuarios */
static pthread_mutex_t users_mutex =
    PTHREAD_MUTEX_INITIALIZER; /* mutex de la tabla */
static int server_fd = -1;     /* socket del servidor */

/* ===================== UTILIDADES DE PROTOCOLO ===================== */

/*
 * Envia una cadena terminada en '\0' por el socket.
 * Devuelve 0 si OK, -1 si error.
 */
static int send_string(int fd, const char *str) {
  size_t len = strlen(str) + 1; /* incluye el '\0' */
  ssize_t sent = send(fd, str, len, 0);
  return (sent == (ssize_t)len) ? 0 : -1;
}

/*
 * Recibe una cadena terminada en '\0' del socket.
 * Almacena el resultado en buf (de tamano buf_size).
 * Devuelve 0 si OK, -1 si error o conexion cerrada.
 */
static int recv_string(int fd, char *buf, size_t buf_size) {
  size_t i = 0;
  char c;
  ssize_t n;

  while (1) {
    n = recv(fd, &c, 1, 0);
    if (n <= 0) {
      buf[i] = '\0';
      return -1;
    }
    if (c == '\0') {
      buf[i] = '\0';
      return 0;
    }
    if (i < buf_size - 1) {
      buf[i++] = c;
    } else {
      /* Overflow: abortar para proteger el buffer y evitar desincronizacion */
      buf[i] = '\0';
      return -1;
    }
  }
}

/*
 * Envia un byte de codigo de respuesta al cliente.
 */
static int send_byte(int fd, unsigned char code) {
  ssize_t n = send(fd, &code, 1, 0);
  return (n == 1) ? 0 : -1;
}

/* ===================== BUSQUEDA DE USUARIOS ===================== */

/*
 * Busca un usuario por nombre en la tabla global.
 * IMPORTANTE: llamar con users_mutex bloqueado.
 * Devuelve indice si encontrado, -1 si no existe.
 */
static int find_user(const char *name) {
  for (int i = 0; i < MAX_USERS; i++) {
    if (users[i].active && strcmp(users[i].name, name) == 0) {
      return i;
    }
  }
  return -1;
}

/*
 * Devuelve el primer slot libre de la tabla.
 * IMPORTANTE: llamar con users_mutex bloqueado.
 */
static int find_free_slot(void) {
  for (int i = 0; i < MAX_USERS; i++) {
    if (!users[i].active)
      return i;
  }
  return -1;
}

/* ===================== GESTION DE MENSAJES PENDIENTES ===================== */

/*
 * Crea un nuevo mensaje y lo anade al final de la lista de pendientes
 * del usuario en el indice idx.
 * Asigna el siguiente id correlativo al usuario.
 * Devuelve el id asignado, o 0 si error.
 * IMPORTANTE: llamar con users_mutex bloqueado.
 */
static unsigned int enqueue_message(int idx, const char *sender,
                                    const char *text, const char *filename) {
  Message *msg = (Message *)malloc(sizeof(Message));
  if (!msg)
    return 0;

  /* Calcular siguiente id: unsigned int, cuando llega a UINT_MAX+1 vuelve a 0
   */
  unsigned int next_id = users[idx].last_msg_id + 1;

  msg->id = next_id;
  users[idx].last_msg_id = next_id;

  strncpy(msg->sender, sender, MAX_NAME - 1);
  msg->sender[MAX_NAME - 1] = '\0';
  strncpy(msg->text, text, MAX_MSG_TEXT - 1);
  msg->text[MAX_MSG_TEXT - 1] = '\0';
  strncpy(msg->filename, filename ? filename : "", MAX_FILENAME - 1);
  msg->filename[MAX_FILENAME - 1] = '\0';
  msg->next = NULL;

  /* Insertar al final de la lista */
  if (!users[idx].pending) {
    users[idx].pending = msg;
  } else {
    Message *cur = users[idx].pending;
    while (cur->next)
      cur = cur->next;
    cur->next = msg;
  }

  return next_id;
}

/*
 * Libera todos los mensajes pendientes de un usuario.
 * IMPORTANTE: llamar con users_mutex bloqueado.
 */
static void free_pending(int idx) {
  Message *cur = users[idx].pending;
  while (cur) {
    Message *next = cur->next;
    free(cur);
    cur = next;
  }
  users[idx].pending = NULL;
}

/* ===================== ENVIO DE MENSAJES A CLIENTES ===================== */

/*
 * Conecta al hilo receptor de un cliente y le entrega un mensaje.
 * Protocolo servidor->cliente (seccion 8.6):
 *   1. Conectar a IP:puerto del cliente
 *   2. Enviar "SEND_MESSAGE\0"
 *   3. Enviar remitente\0
 *   4. Enviar id como cadena\0
 *   5. Enviar texto del mensaje\0
 *   6. Cerrar conexion
 * Devuelve 0 si OK, -1 si error.
 */
static int deliver_message(const char *ip, int port, const char *sender,
                           unsigned int msg_id, const char *text) {
  struct sockaddr_in addr;
  int fd;
  char id_str[32];

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
    close(fd);
    return -1;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  snprintf(id_str, sizeof(id_str), "%u", msg_id);

  if (send_string(fd, OP_SEND_MESSAGE) < 0 || send_string(fd, sender) < 0 ||
      send_string(fd, id_str) < 0 || send_string(fd, text) < 0) {
    close(fd);
    return -1;
  }

  close(fd);
  return 0;
}

/*
 * Entrega un mensaje con fichero adjunto al cliente (Parte 2).
 * Protocolo: "SEND_MESSAGE_ATTACH\0" sender\0 id\0 text\0 filename\0
 */
static int deliver_message_attach(const char *ip, int port, const char *sender,
                                  unsigned int msg_id, const char *text,
                                  const char *filename) {
  struct sockaddr_in addr;
  int fd;
  char id_str[32];

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
    close(fd);
    return -1;
  }
  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  snprintf(id_str, sizeof(id_str), "%u", msg_id);

  if (send_string(fd, OP_SEND_MESSAGE_ATTACH) < 0 ||
      send_string(fd, sender) < 0 || send_string(fd, id_str) < 0 ||
      send_string(fd, text) < 0 || send_string(fd, filename) < 0) {
    close(fd);
    return -1;
  }

  close(fd);
  return 0;
}

/*
 * Notifica al remitente que su mensaje fue entregado (ACK).
 * Protocolo: "SEND_MESS_ACK\0" id\0
 */
static int send_ack(const char *ip, int port, unsigned int msg_id) {
  struct sockaddr_in addr;
  int fd;
  char id_str[32];

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
    close(fd);
    return -1;
  }
  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  snprintf(id_str, sizeof(id_str), "%u", msg_id);

  if (send_string(fd, OP_SEND_MESS_ACK) < 0 || send_string(fd, id_str) < 0) {
    close(fd);
    return -1;
  }

  close(fd);
  return 0;
}

/*
 * Notifica al remitente que su mensaje con adjunto fue entregado (ACK Parte 2).
 * Protocolo: "SEND_MESS_ATTACH_ACK\0" id\0 filename\0
 */
static int send_attach_ack(const char *ip, int port, unsigned int msg_id,
                           const char *filename) {
  struct sockaddr_in addr;
  int fd;
  char id_str[32];

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
    close(fd);
    return -1;
  }
  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  snprintf(id_str, sizeof(id_str), "%u", msg_id);

  if (send_string(fd, OP_SEND_MESS_ATTACH_ACK) < 0 ||
      send_string(fd, id_str) < 0 || send_string(fd, filename) < 0) {
    close(fd);
    return -1;
  }

  close(fd);
  return 0;
}

/* ===================== HANDLERS DE OPERACIONES ===================== */

/*
 * Maneja la operacion REGISTER.
 * Verifica que no exista el usuario, lo registra e inicializa su estado.
 */
static void handle_register(int client_fd,
                            const char *client_ip __attribute__((unused))) {
  char name[MAX_NAME];

  if (recv_string(client_fd, name, sizeof(name)) < 0)
    return;

  pthread_mutex_lock(&users_mutex);

  int idx = find_user(name);
  if (idx >= 0) {
    /* Usuario ya existe */
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_USER_ERROR);
    printf("s> REGISTER %s FAIL\n", name);
    fflush(stdout);
    return;
  }

  int slot = find_free_slot();
  if (slot < 0) {
    /* No hay espacio */
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_ERROR);
    printf("s> REGISTER %s FAIL\n", name);
    fflush(stdout);
    return;
  }

  /* Inicializar entrada */
  memset(&users[slot], 0, sizeof(User));
  users[slot].active = 1;
  users[slot].connected = 0;
  users[slot].last_msg_id = 0;
  users[slot].pending = NULL;
  strncpy(users[slot].name, name, MAX_NAME - 1);

  pthread_mutex_unlock(&users_mutex);

  send_byte(client_fd, RC_OK);
  printf("s> REGISTER %s OK\n", name);
  fflush(stdout);

  /* Notificar al servidor RPC (Parte 2) */
  rpc_log_operation(name, "REGISTER", NULL);
}

/*
 * Maneja la operacion UNREGISTER.
 * Borra al usuario y sus mensajes pendientes.
 */
static void handle_unregister(int client_fd,
                              const char *client_ip __attribute__((unused))) {
  char name[MAX_NAME];

  if (recv_string(client_fd, name, sizeof(name)) < 0)
    return;

  pthread_mutex_lock(&users_mutex);

  int idx = find_user(name);
  if (idx < 0) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_USER_ERROR);
    printf("s> UNREGISTER %s FAIL\n", name);
    fflush(stdout);
    return;
  }

  /* Borrar mensajes pendientes y marcar slot libre */
  free_pending(idx);
  memset(&users[idx], 0, sizeof(User));
  users[idx].active = 0;

  pthread_mutex_unlock(&users_mutex);

  send_byte(client_fd, RC_OK);
  printf("s> UNREGISTER %s OK\n", name);
  fflush(stdout);

  /* Notificar al servidor RPC (Parte 2) */
  rpc_log_operation(name, "UNREGISTER", NULL);
}

/*
 * Maneja la operacion CONNECT.
 * Registra la IP y puerto del hilo receptor del cliente.
 * Si hay mensajes pendientes, los entrega uno a uno.
 */
static void handle_connect(int client_fd, const char *client_ip) {
  char name[MAX_NAME];
  char port_str[16];

  if (recv_string(client_fd, name, sizeof(name)) < 0)
    return;
  if (recv_string(client_fd, port_str, sizeof(port_str)) < 0)
    return;

  int port = atoi(port_str);

  pthread_mutex_lock(&users_mutex);

  int idx = find_user(name);
  if (idx < 0) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_USER_ERROR); /* 1: no existe */
    printf("s> CONNECT %s FAIL\n", name);
    fflush(stdout);
    return;
  }

  if (users[idx].connected) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_ALREADY); /* 2: ya conectado */
    printf("s> CONNECT %s FAIL\n", name);
    fflush(stdout);
    return;
  }

  /* Actualizar estado del usuario */
  users[idx].connected = 1;
  users[idx].port = port;
  strncpy(users[idx].ip, client_ip, MAX_IP - 1);
  users[idx].ip[MAX_IP - 1] = '\0';

  pthread_mutex_unlock(&users_mutex);

  send_byte(client_fd, RC_OK);
  printf("s> CONNECT %s OK\n", name);
  fflush(stdout);

  /* Notificar al servidor RPC (Parte 2) */
  rpc_log_operation(name, "CONNECT", NULL);

  /* Entregar mensajes pendientes uno a uno */
  pthread_mutex_lock(&users_mutex);
  Message *msg = users[idx].pending;
  users[idx].pending = NULL; /* desligar lista antes de soltar el mutex */
  char ip_copy[MAX_IP];
  strncpy(ip_copy, users[idx].ip, MAX_IP - 1);
  int port_copy = users[idx].port;
  pthread_mutex_unlock(&users_mutex);

  Message *fail_head = NULL;
  Message *fail_tail = NULL;

  while (msg) {
    Message *next = msg->next;
    int delivered = 0;

    if (strlen(msg->filename) > 0) {
      /* Mensaje con adjunto */
      delivered =
          (deliver_message_attach(ip_copy, port_copy, msg->sender, msg->id,
                                  msg->text, msg->filename) == 0);
    } else {
      /* Mensaje simple */
      delivered = (deliver_message(ip_copy, port_copy, msg->sender, msg->id,
                                   msg->text) == 0);
    }

    if (delivered) {
      printf("s> SEND MESSAGE %u FROM %s TO %s\n", msg->id, msg->sender, name);
      fflush(stdout);

      /* Notificar al remitente si esta conectado */
      pthread_mutex_lock(&users_mutex);
      int sidx = find_user(msg->sender);
      if (sidx >= 0 && users[sidx].connected) {
        char sip[MAX_IP];
        int sport;
        strncpy(sip, users[sidx].ip, MAX_IP - 1);
        sip[MAX_IP - 1] = '\0';
        sport = users[sidx].port;
        pthread_mutex_unlock(&users_mutex);

        if (strlen(msg->filename) > 0) {
          send_attach_ack(sip, sport, msg->id, msg->filename);
        } else {
          send_ack(sip, sport, msg->id);
        }
      } else {
        pthread_mutex_unlock(&users_mutex);
      }

      free(msg);
    } else {
      /* No se pudo entregar: encolar en lista local de fallidos */
      msg->next = NULL;
      if (!fail_head) {
        fail_head = msg;
        fail_tail = msg;
      } else {
        fail_tail->next = msg;
        fail_tail = msg;
      }
    }

    msg = next;
  }

  if (fail_head) {
    /* Re-insertar todos los fallidos al principio para mantener el orden */
    pthread_mutex_lock(&users_mutex);
    fail_tail->next = users[idx].pending;
    users[idx].pending = fail_head;
    pthread_mutex_unlock(&users_mutex);
  }
}

/*
 * Maneja la operacion DISCONNECT.
 * Marca al usuario como desconectado y borra su IP/puerto.
 */
static void handle_disconnect(int client_fd,
                              const char *client_ip __attribute__((unused))) {
  char name[MAX_NAME];

  if (recv_string(client_fd, name, sizeof(name)) < 0)
    return;

  pthread_mutex_lock(&users_mutex);

  int idx = find_user(name);
  if (idx < 0) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_USER_ERROR); /* 1: no existe */
    printf("s> DISCONNECT %s FAIL\n", name);
    fflush(stdout);
    return;
  }

  if (!users[idx].connected) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_NOT_CONN); /* 2: no conectado */
    printf("s> DISCONNECT %s FAIL\n", name);
    fflush(stdout);
    return;
  }

  users[idx].connected = 0;
  memset(users[idx].ip, 0, sizeof(users[idx].ip));
  users[idx].port = 0;

  pthread_mutex_unlock(&users_mutex);

  send_byte(client_fd, RC_OK);
  printf("s> DISCONNECT %s OK\n", name);
  fflush(stdout);

  /* Notificar al servidor RPC (Parte 2) */
  rpc_log_operation(name, "DISCONNECT", NULL);
}

/*
 * Maneja la operacion SEND.
 * Almacena el mensaje, responde con el id y entrega si el destinatario esta
 * conectado.
 */
static void handle_send(int client_fd,
                        const char *client_ip __attribute__((unused))) {
  char sender[MAX_NAME], dest[MAX_NAME], text[MAX_MSG_TEXT];

  if (recv_string(client_fd, sender, sizeof(sender)) < 0)
    return;
  if (recv_string(client_fd, dest, sizeof(dest)) < 0)
    return;
  if (recv_string(client_fd, text, sizeof(text)) < 0)
    return;

  pthread_mutex_lock(&users_mutex);

  int didx = find_user(dest);
  int sidx = find_user(sender);

  if (didx < 0 || sidx < 0) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_USER_ERROR); /* 1: usuario no existe */
    return;
  }

  /* Asignar id y encolar el mensaje */
  unsigned int msg_id = enqueue_message(didx, sender, text, NULL);
  if (msg_id == 0) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_ERROR);
    return;
  }

  /* Responder al remitente con el id */
  char id_str[32];
  snprintf(id_str, sizeof(id_str), "%u", msg_id);

  int dest_connected = users[didx].connected;
  char dest_ip[MAX_IP];
  int dest_port;
  strncpy(dest_ip, users[didx].ip, MAX_IP - 1);
  dest_ip[MAX_IP - 1] = '\0';
  dest_port = users[didx].port;

  int sender_connected = users[sidx].connected;
  char sender_ip[MAX_IP];
  int sender_port;
  strncpy(sender_ip, users[sidx].ip, MAX_IP - 1);
  sender_ip[MAX_IP - 1] = '\0';
  sender_port = users[sidx].port;

  pthread_mutex_unlock(&users_mutex);

  send_byte(client_fd, RC_OK);
  send_string(client_fd, id_str);

  /* Notificar al servidor RPC (Parte 2) */
  rpc_log_operation(sender, "SEND", NULL);

  if (dest_connected) {
    /* Entregar inmediatamente */
    if (deliver_message(dest_ip, dest_port, sender, msg_id, text) == 0) {
      /* Eliminar de pendientes */
      pthread_mutex_lock(&users_mutex);
      Message *prev = NULL;
      Message *cur = users[didx].pending;
      while (cur) {
        if (cur->id == msg_id) {
          if (prev)
            prev->next = cur->next;
          else
            users[didx].pending = cur->next;
          free(cur);
          break;
        }
        prev = cur;
        cur = cur->next;
      }
      pthread_mutex_unlock(&users_mutex);

      printf("s> SEND MESSAGE %u FROM %s TO %s\n", msg_id, sender, dest);
      fflush(stdout);

      /* ACK al remitente si esta conectado */
      if (sender_connected) {
        send_ack(sender_ip, sender_port, msg_id);
      }
    } else {
      /* Error de entrega: marcar destinatario como desconectado solo si no ha
       * reconectado */
      pthread_mutex_lock(&users_mutex);
      if (users[didx].port == dest_port &&
          strcmp(users[didx].ip, dest_ip) == 0) {
        users[didx].connected = 0;
      }
      pthread_mutex_unlock(&users_mutex);
      printf("s> MESSAGE %u FROM %s TO %s STORED\n", msg_id, sender, dest);
      fflush(stdout);
    }
  } else {
    printf("s> MESSAGE %u FROM %s TO %s STORED\n", msg_id, sender, dest);
    fflush(stdout);
  }
}

/*
 * Maneja la operacion SENDATTACH (Parte 2).
 * Identica a SEND pero incluye el nombre del fichero adjunto.
 */
static void handle_sendattach(int client_fd,
                              const char *client_ip __attribute__((unused))) {
  char sender[MAX_NAME], dest[MAX_NAME], text[MAX_MSG_TEXT],
      filename[MAX_FILENAME];

  if (recv_string(client_fd, sender, sizeof(sender)) < 0)
    return;
  if (recv_string(client_fd, dest, sizeof(dest)) < 0)
    return;
  if (recv_string(client_fd, text, sizeof(text)) < 0)
    return;
  if (recv_string(client_fd, filename, sizeof(filename)) < 0)
    return;

  pthread_mutex_lock(&users_mutex);

  int didx = find_user(dest);
  int sidx = find_user(sender);

  if (didx < 0 || sidx < 0) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_USER_ERROR);
    return;
  }

  unsigned int msg_id = enqueue_message(didx, sender, text, filename);
  if (msg_id == 0) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_ERROR);
    return;
  }

  char id_str[32];
  snprintf(id_str, sizeof(id_str), "%u", msg_id);

  int dest_connected = users[didx].connected;
  char dest_ip[MAX_IP];
  int dest_port;
  strncpy(dest_ip, users[didx].ip, MAX_IP - 1);
  dest_ip[MAX_IP - 1] = '\0';
  dest_port = users[didx].port;

  int sender_connected = users[sidx].connected;
  char sender_ip[MAX_IP];
  int sender_port;
  strncpy(sender_ip, users[sidx].ip, MAX_IP - 1);
  sender_ip[MAX_IP - 1] = '\0';
  sender_port = users[sidx].port;

  pthread_mutex_unlock(&users_mutex);

  send_byte(client_fd, RC_OK);
  send_string(client_fd, id_str);

  /* Notificar al servidor RPC (Parte 2) - incluye nombre de fichero */
  rpc_log_operation(sender, "SENDATTACH", filename);

  if (dest_connected) {
    if (deliver_message_attach(dest_ip, dest_port, sender, msg_id, text,
                               filename) == 0) {
      pthread_mutex_lock(&users_mutex);
      Message *prev = NULL, *cur = users[didx].pending;
      while (cur) {
        if (cur->id == msg_id) {
          if (prev)
            prev->next = cur->next;
          else
            users[didx].pending = cur->next;
          free(cur);
          break;
        }
        prev = cur;
        cur = cur->next;
      }
      pthread_mutex_unlock(&users_mutex);

      printf("s> SEND MESSAGE %u FROM %s TO %s\n", msg_id, sender, dest);
      fflush(stdout);

      if (sender_connected) {
        send_attach_ack(sender_ip, sender_port, msg_id, filename);
      }
    } else {
      pthread_mutex_lock(&users_mutex);
      if (users[didx].port == dest_port &&
          strcmp(users[didx].ip, dest_ip) == 0) {
        users[didx].connected = 0;
      }
      pthread_mutex_unlock(&users_mutex);
      printf("s> MESSAGE %u FROM %s TO %s STORED\n", msg_id, sender, dest);
      fflush(stdout);
    }
  } else {
    printf("s> MESSAGE %u FROM %s TO %s STORED\n", msg_id, sender, dest);
    fflush(stdout);
  }
}

/*
 * Maneja la operacion USERS.
 * Devuelve la lista de usuarios conectados al cliente que lo solicita.
 * En Parte 2, cada entrada tiene formato "usuario :: IP :: puerto".
 */
static void handle_users(int client_fd,
                         const char *client_ip __attribute__((unused))) {
  char requester[MAX_NAME];

  if (recv_string(client_fd, requester, sizeof(requester)) < 0)
    return;

  pthread_mutex_lock(&users_mutex);

  int ridx = find_user(requester);
  if (ridx < 0) {
    /* Usuario no registrado */
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_ERROR);
    printf("s> CONNECTED USERS FAIL\n");
    fflush(stdout);
    return;
  }

  if (!users[ridx].connected) {
    pthread_mutex_unlock(&users_mutex);
    send_byte(client_fd, RC_USER_ERROR); /* 1: no conectado */
    printf("s> CONNECTED USERS FAIL\n");
    fflush(stdout);
    return;
  }

  /* Contar y recopilar usuarios conectados */
  int count = 0;
  char entries[MAX_USERS][MAX_NAME + MAX_IP + 32];

  for (int i = 0; i < MAX_USERS; i++) {
    if (users[i].active && users[i].connected) {
      /* Formato Parte 2: "usuario :: IP :: puerto" */
      snprintf(entries[count], sizeof(entries[count]), "%s :: %s :: %d",
               users[i].name, users[i].ip, users[i].port);
      count++;
    }
  }

  pthread_mutex_unlock(&users_mutex);

  /* Enviar respuesta */
  char count_str[16];
  snprintf(count_str, sizeof(count_str), "%d", count);

  send_byte(client_fd, RC_OK);
  send_string(client_fd, count_str);
  for (int i = 0; i < count; i++) {
    send_string(client_fd, entries[i]);
  }

  /* Notificar al servidor RPC (Parte 2) */
  rpc_log_operation(requester, "USERS", NULL);

  printf("s> CONNECTED USERS OK\n");
  fflush(stdout);
}

/* ===================== HILO POR CLIENTE ===================== */

/*
 * Datos pasados al hilo de atencion a cada cliente.
 */
typedef struct {
  int fd;          /* socket del cliente */
  char ip[MAX_IP]; /* IP del cliente (obtenida via accept) */
} ClientArgs;

/*
 * Hilo que atiende una conexion de un cliente.
 * Lee la operacion y la despacha al handler correspondiente.
 */
static void *client_thread(void *arg) {
  ClientArgs *ca = (ClientArgs *)arg;
  int fd = ca->fd;
  char ip[MAX_IP];
  strncpy(ip, ca->ip, MAX_IP - 1);
  ip[MAX_IP - 1] = '\0';
  free(ca);

  char operation[MAX_NAME];
  if (recv_string(fd, operation, sizeof(operation)) < 0) {
    close(fd);
    return NULL;
  }

  if (strcmp(operation, OP_REGISTER) == 0)
    handle_register(fd, ip);
  else if (strcmp(operation, OP_UNREGISTER) == 0)
    handle_unregister(fd, ip);
  else if (strcmp(operation, OP_CONNECT) == 0)
    handle_connect(fd, ip);
  else if (strcmp(operation, OP_DISCONNECT) == 0)
    handle_disconnect(fd, ip);
  else if (strcmp(operation, OP_SEND) == 0)
    handle_send(fd, ip);
  else if (strcmp(operation, OP_SENDATTACH) == 0)
    handle_sendattach(fd, ip);
  else if (strcmp(operation, OP_USERS) == 0)
    handle_users(fd, ip);
  else {
    fprintf(stderr, "s> Unknown operation: %s\n", operation);
  }

  close(fd);
  return NULL;
}

/* ===================== SIGNAL HANDLER ===================== */

/*
 * Libera todos los mensajes pendientes de todos los usuarios (para apagado
 * limpio).
 */
static void free_all_pending_messages(void) {
  pthread_mutex_lock(&users_mutex);
  for (int i = 0; i < MAX_USERS; i++) {
    if (users[i].active) {
      free_pending(i);
    }
  }
  pthread_mutex_unlock(&users_mutex);
}

/*
 * Captura SIGINT (Ctrl+C) para cerrar el servidor limpiamente.
 */
static void sigint_handler(int sig) {
  (void)sig;
  printf("\ns> Shutting down server...\n");
  if (server_fd >= 0)
    close(server_fd);
  free_all_pending_messages();
  exit(0);
}

/* ===================== MAIN ===================== */

int main(int argc, char *argv[]) {
  int port = -1;

  /* Parsear argumentos: ./server -p <port> */
  for (int i = 1; i < argc - 1; i++) {
    if (strcmp(argv[i], "-p") == 0) {
      port = atoi(argv[i + 1]);
    }
  }

  if (port < 1024 || port > 65535) {
    fprintf(stderr, "Usage: %s -p <port>\n", argv[0]);
    return 1;
  }

  /* Instalar manejador de SIGINT */
  signal(SIGINT, sigint_handler);

  /* Inicializar tabla de usuarios */
  memset(users, 0, sizeof(users));

  /* Crear socket del servidor */
  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    perror("socket");
    return 1;
  }

  /* Permitir reusar el puerto rapidamente tras reinicio */
  int opt = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons((uint16_t)port);

  if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    close(server_fd);
    return 1;
  }

  if (listen(server_fd, BACKLOG) < 0) {
    perror("listen");
    close(server_fd);
    return 1;
  }

  /* Obtener IP local para mostrarla en el inicio */
  char local_ip[MAX_IP] = "0.0.0.0";
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) == 0) {
    struct hostent *he = gethostbyname(hostname);
    if (he && he->h_addr_list[0]) {
      inet_ntop(AF_INET, he->h_addr_list[0], local_ip, sizeof(local_ip));
    }
  }

  printf("s> init server %s:%d\n", local_ip, port);
  printf("s> \n");
  fflush(stdout);

  /* Bucle principal: aceptar conexiones y lanzar hilos */
  while (1) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd =
        accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
    if (client_fd < 0) {
      if (errno == EINTR)
        continue; /* senyal recibida, reintentar */
      perror("accept");
      break;
    }

    /* Obtener IP del cliente */
    char client_ip[MAX_IP];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));

    /* Preparar argumentos para el hilo */
    ClientArgs *ca = (ClientArgs *)malloc(sizeof(ClientArgs));
    if (!ca) {
      close(client_fd);
      continue;
    }
    ca->fd = client_fd;
    strncpy(ca->ip, client_ip, MAX_IP - 1);
    ca->ip[MAX_IP - 1] = '\0';

    /* Lanzar hilo detached (se limpia solo al terminar) */
    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&tid, &attr, client_thread, ca) != 0) {
      perror("pthread_create");
      free(ca);
      close(client_fd);
    }
    pthread_attr_destroy(&attr);
  }

  close(server_fd);
  return 0;
}
