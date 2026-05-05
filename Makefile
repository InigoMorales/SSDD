# Makefile - Servicio de mensajeria distribuido
# Sistemas Distribuidos - UC3M - Curso 2025-2026
#
# Targets:
#   make           -> compila servidor de mensajeria (con soporte RPC)
#   make logger    -> compila servidor RPC de registro
#   make all       -> compila todo
#   make clean     -> elimina ejecutables y objetos generados

CC      = gcc
CFLAGS  = -Wall -Wextra -g -pthread -I/usr/include/tirpc

# ── Servidor de mensajeria (server.c + stub cliente RPC) ──────────────────
SERVER_TARGET = server
SERVER_SRCS   = server.c rpc_client.c logger_clnt.c logger_xdr.c
SERVER_OBJS   = $(SERVER_SRCS:.c=.o)

# ── Servidor RPC de registro (stub servidor + implementacion) ─────────────
LOGGER_TARGET = logger_server
LOGGER_SRCS   = logger_server_impl.c logger_svc.c logger_xdr.c
LOGGER_OBJS   = $(LOGGER_SRCS:.c=.o)

.PHONY: all clean logger

all: $(SERVER_TARGET) $(LOGGER_TARGET)

# Compilar servidor de mensajeria
$(SERVER_TARGET): $(SERVER_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ -ltirpc

# Compilar servidor RPC
$(LOGGER_TARGET): $(LOGGER_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ -ltirpc

# Regla generica para objetos propios (con todos los warnings)
%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# Ficheros generados automaticamente por rpcgen: tienen warnings
# inevitables (variable no usada, cast de funcion). Se compilan
# sin -Wextra para no penalizar, pero mantienen -Wall.
RPCGEN_FLAGS = -Wall -g -I/usr/include/tirpc

logger_clnt.o: logger_clnt.c
	$(CC) $(RPCGEN_FLAGS) -c -o $@ $<

logger_xdr.o: logger_xdr.c
	$(CC) $(RPCGEN_FLAGS) -Wno-unused-variable -c -o $@ $<

logger_svc.o: logger_svc.c
	$(CC) $(RPCGEN_FLAGS) -c -o $@ $<

# Limpiar
clean:
	rm -f $(SERVER_TARGET) $(LOGGER_TARGET) *.o

