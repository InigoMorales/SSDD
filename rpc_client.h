/*
 * rpc_client.h - Cabecera del modulo de logging RPC
 * Sistemas Distribuidos - UC3M - Curso 2025-2026
 */

#ifndef RPC_CLIENT_H
#define RPC_CLIENT_H

/*
 * rpc_log_operation - Notifica al servidor RPC una operacion de usuario.
 *
 * @username:  nombre del usuario
 * @operation: nombre de la operacion (REGISTER, UNREGISTER, CONNECT,
 *             DISCONNECT, USERS, SEND, SENDATTACH)
 * @filename:  nombre del fichero adjunto para SENDATTACH, NULL o "" si no aplica
 *
 * Lee LOG_RPC_IP del entorno. Si no esta definida, no hace nada.
 */
void rpc_log_operation(const char *username, const char *operation, const char *filename);

#endif /* RPC_CLIENT_H */
