/*
 * logger_server_impl.c - Implementacion del servidor RPC de registro
 * Sistemas Distribuidos - UC3M - Curso 2025-2026
 *
 * Este fichero implementa la funcion log_operation_1_svc que es llamada
 * automaticamente por el stub generado por rpcgen (logger_svc.c) cada vez
 * que el servidor de mensajeria realiza una llamada RPC.
 *
 * Imprime por pantalla: Nombre_usuario   OPERACION [fichero]
 */

#include "logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Resultado estatico que devuelve la funcion RPC (requerido por ONC-RPC) */
static int result;

/*
 * log_operation_1_svc - Registra una operacion de un usuario.
 *
 * Llamada por el stub del servidor cada vez que el servidor de mensajeria
 * invoca log_operation via RPC.
 *
 * Formato de salida:
 *   Nombre_usuario   OPERACION
 *   Nombre_usuario   SENDATTACH /tmp/file.txt   (si hay fichero adjunto)
 *
 * Devuelve puntero a int con valor 0 si OK.
 */
int *
log_operation_1_svc(log_args *argp, struct svc_req *rqstp __attribute__((unused)))
{
    result = 0;

    if (!argp || !argp->username || !argp->operation) {
        result = -1;
        return &result;
    }

    /*
     * Imprimir segun el tipo de operacion.
     * Para SENDATTACH se incluye el nombre del fichero.
     */
    if (strcmp(argp->operation, "SENDATTACH") == 0 &&
        argp->filename && strlen(argp->filename) > 0) {
        printf("%s\t\tSENDATTACH %s\n", argp->username, argp->filename);
    } else {
        printf("%s\t\t%s\n", argp->username, argp->operation);
    }
    fflush(stdout);

    return &result;
}

/*
 * logger_prog_1_freeresult - Libera recursos tras cada llamada RPC.
 * Requerido por el framework ONC-RPC.
 */
int
logger_prog_1_freeresult(SVCXPRT *transp __attribute__((unused)),
                          xdrproc_t xdr_result __attribute__((unused)),
                          caddr_t result __attribute__((unused)))
{
    /* No hay memoria dinamica que liberar en este servicio */
    return 1;
}
