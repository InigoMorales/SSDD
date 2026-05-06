"""
web_service.py - Conversor de mensajes (Servicio Web)
Sistemas Distribuidos - UC3M - Curso 2025-2026 - Parte 2

Servicio web que normaliza los mensajes enviados por los usuarios:
elimina espacios en blanco repetidos para que las palabras queden
separadas por un unico espacio.

Uso: python3 web_service.py
     Escucha por defecto en http://localhost:8080/normalize
"""

import re
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import urllib.parse

# Puerto donde escucha el servicio web
WEB_PORT = 8080


def normalize_message(text):
    """
    Elimina espacios en blanco repetidos del mensaje.
    Tambien elimina espacios al inicio y al final.
    """
    return re.sub(r' +', ' ', text).strip()


class NormalizeHandler(BaseHTTPRequestHandler):
    """
    Handler HTTP que atiende peticiones POST a /normalize.
    Espera JSON con campo 'message' y devuelve JSON con 'normalized'.
    """

    def log_message(self, format, *args):
        """Silenciar los logs por defecto del servidor HTTP."""
        pass

    def do_POST(self):
        """
        Atiende peticiones POST /normalize.
        Body esperado:  {"message": "texto  con   espacios"}
        Respuesta:      {"normalized": "texto con espacios"}
        """
        if self.path != '/normalize':
            self._send_error(404, "Not Found")
            return

        try:
            # Leer el body de la peticion
            length   = int(self.headers.get('Content-Length', 0))
            if length > 1024 * 1024:  # Limite estricto de 1MB para prevenir DoS
                self._send_error(413, "Payload Too Large")
                return
            raw_body = self.rfile.read(length)
            body     = json.loads(raw_body.decode('utf-8'))

            message    = body.get('message', '')
            normalized = normalize_message(message)

            # Construir respuesta JSON
            response = json.dumps({'normalized': normalized}).encode('utf-8')

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        except Exception as e:
            self._send_error(400, str(e))

    def do_GET(self):
        """
        Endpoint de health-check: GET /health devuelve 200 OK.
        """
        if self.path == '/health':
            response = b'{"status": "ok"}'
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        else:
            self._send_error(404, "Not Found")

    def _send_error(self, code, message):
        """Envia una respuesta de error HTTP."""
        response = json.dumps({'error': message}).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)


def call_normalize(message, host='localhost', port=WEB_PORT):
    """
    Funcion auxiliar para que el cliente llame al servicio web.
    Devuelve el mensaje normalizado, o el original si hay error.
    """
    import http.client
    import json

    try:
        body = json.dumps({'message': message}).encode('utf-8')
        conn = http.client.HTTPConnection(host, port, timeout=2)
        conn.request('POST', '/normalize',
                     body=body,
                     headers={'Content-Type': 'application/json',
                               'Content-Length': str(len(body))})
        resp = conn.getresponse()
        if resp.status == 200:
            data = json.loads(resp.read().decode('utf-8'))
            return data.get('normalized', message)
        conn.close()
    except Exception:
        pass
    return message  # fallback: devolver original si el servicio no esta disponible


if __name__ == '__main__':
    print(f"s> Conversor de mensajes escuchando en http://localhost:{WEB_PORT}")
    print(f"   Endpoint: POST /normalize")
    print(f"   Health:   GET  /health")
    httpd = HTTPServer(('', WEB_PORT), NormalizeHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\ns> Servicio web detenido.")
        httpd.server_close()
