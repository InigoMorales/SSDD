/*
 * rpc_client.c - Funciones auxiliares para que server.c llame al servicio RPC
 * Sistemas Distribuidos - UC3M - Curso 2025-2026
 *
 * El servidor de mensajeria (server.c) usa estas funciones para notificar
 * al servidor RPC de registro cada operacion que recibe.
 *
 * La IP del servidor RPC se obtiene de la variable de entorno LOG_RPC_IP.
 * Si no esta definida, el logging RPC se omite silenciosamente.
 */

#include "rpc_client.h"
#include "logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * rpc_log_operation - Envia una operacion al servidor RPC de registro.
 *
 * Parametros:
 *   username  - nombre del usuario que realiza la operacion
 *   operation - nombre de la operacion (REGISTER, CONNECT, SEND, etc.)
 *   filename  - nombre del fichero adjunto (solo para SENDATTACH, NULL o "" si no aplica)
 *
 * Obtiene la IP del servidor RPC de la variable de entorno LOG_RPC_IP.
 * Si la variable no esta definida o la llamada falla, lo ignora (no es critico).
 */
void rpc_log_operation(const char *username, const char *operation, const char *filename)
{
    /* Obtener la IP del servidor RPC desde la variable de entorno */
    const char *rpc_ip = getenv("LOG_RPC_IP");
    if (!rpc_ip || strlen(rpc_ip) == 0) {
        /* Variable no definida: el logging RPC esta deshabilitado */
        return;
    }

    /* Crear handle de cliente RPC */
    CLIENT *clnt = clnt_create(rpc_ip, LOGGER_PROG, LOGGER_VERS, "tcp");
    if (!clnt) {
        /* No se puede conectar al servidor RPC: continuar sin logging */
        return;
    }

    /* Preparar argumentos */
    log_args args;
    args.username  = (char *)username;
    args.operation = (char *)operation;
    args.filename  = (char *)(filename ? filename : "");

    /* Realizar la llamada RPC */
    int *result = log_operation_1(&args, clnt);
    if (!result) {
        /* La llamada fallo: continuar sin logging */
        clnt_perror(clnt, "rpc_log_operation");
    }

    /* Liberar el handle */
    clnt_destroy(clnt);
}
