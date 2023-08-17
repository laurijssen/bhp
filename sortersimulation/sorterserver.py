import json
import socket
import threading

class SorterScan(object):
    def __init__(self, jsonstring):
        self.__dict__ = json.loads(jsonstring)    

def ec30_sock(ip):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, 0)
    s.connect((ip, 3333))
    return s

def socket_server():

    PORT = 3333
    ips = [i[4][0] for i in socket.getaddrinfo(socket.gethostname(), None)]

    serverip = [i for i in ips if '10.203' in i][0]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((serverip, PORT))
        server.listen()
        while True:
            conn, addr = server.accept()
            with conn:
                result = ''
                while True:
                    chunk = conn.recv(1024)
                    if chunk == b'':
                        break #raise RuntimeError("socket connection broken")
                    result += chunk.decode('ascii')

            sscan = SorterScan(result)

            parsed = json.loads(result)
            print(json.dumps(parsed, indent=4, sort_keys=True))

            if sscan.SorterBagTypes:
                with ec30_sock('192.168.30.29') as s:
                    s.send(result.encode('ascii'))
                    print('sending sticker')
            elif sscan.ZPL is not None and len(sscan.ZPL) > 0:
                with ec30_sock('192.168.30.29') as s:
                    s.send(result.encode('ascii'))
                    print('sending sticker')
            elif sscan.PDF is not None and len(sscan.PDF) > 0:
                with ec30_sock('192.168.30.29') as s:
                    s.send(result.encode('ascii'))
                    print('sending sticker')

def start_thread():
    t = threading.Thread(target=socket_server)
    t.start()
