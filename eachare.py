#!/usr/bin/env python3
import sys
import os
import socket
import threading
import time
import base64


# Variáveis globais utilizadas para sincronização e controle do servidor
global_clock = 0                      # Relógio global (incrementado a cada evento)
clock_lock = threading.Lock()         # Lock para acesso thread-safe ao global_clock
peers = {}                            # Dicionário com peers: chave = "ip:porta", valor = {"status": "ONLINE" ou "OFFLINE", "clock": valor do relógio}
peers_lock = threading.Lock()         # Lock para acesso thread-safe ao dicionário de peers
running = True                        # Flag que controla a execução do loop do servidor
server_socket = None                  # Socket do servidor para permitir encerramento adequado

# Variáveis configuradas a partir dos parâmetros de entrada
server_id = None                      # Identificador do servidor no formato "ip:porta"
local_ip = None                      # IP local
local_port = None                    # Porta local
shared_dir = None                    # Diretório compartilhado para arquivos

def update_clock():
    """
    Incrementa o relógio global de forma thread-safe e retorna o novo valor.
    """
    global global_clock
    with clock_lock:
        global_clock += 1
        print(f"=> Atualizando relogio para {global_clock}")
        return global_clock

def send_message(peer_addr, message, expect_reply=False, timeout=5, max_retries=2):
    """
    Envia uma mensagem para o peer especificado com tentativas de reenvio.
    
    Parâmetros:
      - peer_addr: string no formato "ip:porta" do destinatário.
      - message: mensagem a ser enviada.
      - expect_reply: se True, aguarda e retorna a resposta do peer.
      - timeout: tempo de espera por resposta em segundos.
      - max_retries: número máximo de tentativas em caso de falha.
      
    Retorna:
      - Resposta recebida, True se a mensagem foi enviada com sucesso sem resposta,
      ou None se todas as tentativas falharam (peer considerado OFFLINE).
    """
    ip, port_str = peer_addr.split(":")
    port = int(port_str)
    
    for attempt in range(max_retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect((ip, port))
                # Incrementa o relógio antes de enviar a mensagem
                msg_clock = update_clock()
                formatted_message = f"{server_id} {msg_clock} {message.strip()}\n"
                print(f'Encaminhando mensagem "{formatted_message.strip()}" para {peer_addr}')
                sock.sendall(formatted_message.encode())
                
                if expect_reply:
                    reply = sock.recv(4096).decode()
                    return reply
                else:
                    return True  # Sucesso sem necessidade de resposta
        except socket.timeout:
            print(f"Timeout ao conectar com {peer_addr} (tentativa {attempt + 1}/{max_retries})")
        except ConnectionRefusedError:
            print(f"Conexão recusada por {peer_addr}")
            return None  # Peer claramente offline
        except Exception as e:
            print(f"Erro ao enviar mensagem para {peer_addr}: {e}")
    
    return None  # Falharam todas as tentativas
def handle_client(conn, addr):
    """
    Lida com a conexão de um peer. Processa a mensagem recebida e executa a ação
    correspondente (HELLO, GET_PEERS, BYE, LS, DL).
    """
    try:
        data = conn.recv(4096).decode()
        if not data:
            return
        data = data.strip()
        print(f"Mensagem recebida: \"{data}\"")
        parts = data.split()
        if len(parts) < 3:
            return
        
        origem = parts[0]
        msg_clock = int(parts[1])
        msg_type = parts[2]

        # Atualiza o relógio local para o maior valor entre o valor atual e o valor contido no cabeçalho da mensagem recebida
        global global_clock
        with clock_lock:
            global_clock = max(global_clock, msg_clock) + 1
            print(f"=> Atualizando relogio para {global_clock}")

        if msg_type == "HELLO":
            # Marca o peer como ONLINE e atualiza o relógio
            with peers_lock:
                if origem not in peers or peers[origem]["status"] != "ONLINE":
                    peers[origem] = {"status": "ONLINE", "clock": msg_clock}
                else:
                    peers[origem]["clock"] = msg_clock
                print(f"Atualizando peer {origem} status ONLINE e relógio para {msg_clock}")
        elif msg_type == "GET_PEERS":
            # Responde com uma lista de peers conhecidos (exceto o remetente)
            with peers_lock:
                lista_peers = [f"{peer}:{info['status']}:{info['clock']}" for peer, info in peers.items() if peer != origem]
                total = len(lista_peers)
            
            reply_msg = f"{server_id} {update_clock()} PEER_LIST {total}"
            if total > 0:
                reply_msg += " " + " ".join(lista_peers)
            reply_msg += "\n"  # Garante a quebra de linha final
            
            try:
                conn.sendall(reply_msg.encode())
            except Exception as e:
                print(f"Erro ao enviar resposta para {origem}: {e}")
                
        elif msg_type == "BYE":
            # Marca o peer como OFFLINE
            with peers_lock:
                if origem in peers and peers[origem]["status"] != "OFFLINE":
                    peers[origem]["status"] = "OFFLINE"
                    print(f"Atualizando peer {origem} status OFFLINE")
        elif msg_type == "LS":
            # Responde com a lista de arquivos compartilhados
            try:
                files = os.listdir(shared_dir)
                files_info = [f"{filename}:{os.path.getsize(os.path.join(shared_dir, filename))}" for filename in files]
                total_files = len(files_info)
                reply_msg = f"{server_id} {update_clock()} LS_LIST {total_files}"
                if total_files > 0:
                    reply_msg += " " + " ".join(files_info)
                reply_msg += "\n"
                conn.sendall(reply_msg.encode())
            except Exception as e:
                print(f"Erro ao enviar resposta LS_LIST para {origem}: {e}")
        elif msg_type == "DL":
            # Envia o arquivo solicitado
            if len(parts) >= 4:
                nome_arquivo = parts[3]
                caminho_arquivo = os.path.join(shared_dir, nome_arquivo)
                if os.path.isfile(caminho_arquivo):
                    with open(caminho_arquivo, "rb") as f:
                        conteudo_binario = f.read()
                    conteudo_base64 = base64.b64encode(conteudo_binario).decode()
                    reply_msg = f"{server_id} {update_clock()} FILE {nome_arquivo} 0 0 {conteudo_base64}\n"
                    conn.sendall(reply_msg.encode())
                else:
                    print(f"Arquivo {nome_arquivo} não encontrado.")
    except Exception as e:
        print(f"Erro no tratamento da conexão de {addr}: {e}")
    finally:
        try:
            conn.close()
        except:
            pass

def command_search_files():
    """
    Varre todos os peers conhecidos com status ONLINE em busca de arquivos compartilhados disponíveis.
    Exibe uma lista compilada de todos os arquivos disponíveis em todos os peers.
    Permite ao usuário escolher um arquivo para fazer o download.
    """
    global global_clock

    # Dicionário para armazenar os arquivos encontrados
    arquivos_disponiveis = []

    with peers_lock:
        online_peers = [peer for peer, info in peers.items() if info["status"] == "ONLINE"]

    for peer_addr in online_peers:
        message = "LS"
        reply = send_message(peer_addr, message, expect_reply=True)

        if reply:
            try:
                parts = reply.strip().split()
                if len(parts) >= 4 and parts[2] == "LS_LIST":
                    total = int(parts[3])
                    arquivos = parts[4:4+total] if total > 0 else []

                    for arquivo_info in arquivos:
                        nome, tamanho = arquivo_info.split(":")
                        arquivos_disponiveis.append((nome, int(tamanho), peer_addr))

                    # Marca o peer que respondeu como ONLINE
                    with peers_lock:
                        if peers[peer_addr]["status"] != "ONLINE":
                            peers[peer_addr]["status"] = "ONLINE"
                            print(f"Atualizando peer {peer_addr} status ONLINE")
                else:
                    print(f"Resposta inválida de {peer_addr}: {reply}")
            except Exception as e:
                print(f"Erro ao processar resposta de {peer_addr}: {e}")
        else:
            with peers_lock:
                if peers[peer_addr]["status"] != "OFFLINE":
                    peers[peer_addr]["status"] = "OFFLINE"
                    print(f"Atualizando peer {peer_addr} status OFFLINE")

    # Exibe a lista de arquivos encontrados
    print("\nArquivos encontrados na rede:")
    print("Nome | Tamanho | Peer")
    print("[ 0] <Cancelar> | |")
    for idx, (nome, tamanho, peer) in enumerate(arquivos_disponiveis, start=1):
        print(f"[{idx}] {nome} | {tamanho} | {peer}")

    escolha = input("Digite o numero do arquivo para fazer o download: ").strip()
    try:
        idx = int(escolha) - 1
        if idx < 0 or idx >= len(arquivos_disponiveis):
            print("Opção inválida!")
            return

        nome_arquivo, _, peer_addr = arquivos_disponiveis[idx]
        # Envia mensagem DL para o peer selecionado
        message = f"DL {nome_arquivo} 0 0"
        reply = send_message(peer_addr, message, expect_reply=True)

        if reply:
            print(f"Resposta recebida: \"{reply}\"")  # Adiciona mensagem de depuração
            parts = reply.strip().split()
            if len(parts) >= 6 and parts[2] == "FILE":
                nome_arquivo = parts[3]
                conteudo_base64 = parts[5]
                if conteudo_base64:  # Verifica se o conteúdo não está vazio
                    conteudo_binario = base64.b64decode(conteudo_base64)

                    # Salva o arquivo no diretório compartilhado
                    caminho_arquivo = os.path.join(shared_dir, nome_arquivo)
                    with open(caminho_arquivo, "wb") as f:
                        f.write(conteudo_binario)

                    print(f"Download do arquivo {nome_arquivo} finalizado.")
                else:
                    print(f"Conteúdo do arquivo {nome_arquivo} está vazio.")
            else:
                print(f"Resposta inválida de {peer_addr}: {reply}")
        else:
            print(f"Falha ao baixar o arquivo de {peer_addr}")

    except ValueError:
        print("Digite um número válido2!")

def server_thread():
    """ 
    Executa o servidor TCP que escuta conexões de outros peers.
    Configura o socket, aceita conexões e inicia uma thread para cada cliente.
    """
    global server_socket, running
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((local_ip, local_port))
        server_socket.listen(5)
        server_socket.settimeout(1)  # Permite verificar periodicamente a flag 'running'
        
        print(f"Servidor iniciado em {local_ip}:{local_port}")
        
        while running:
            try:
                conn, addr = server_socket.accept()
                threading.Thread(target=handle_client, args=(conn, addr)).start()
            except socket.timeout:
                continue
            except Exception as e:
                if running:
                    print(f"Erro ao aceitar conexão: {e}")
                break
    except Exception as e:
        print(f"Erro ao iniciar servidor: {e}")
        running = False
    finally:
        if server_socket:
            server_socket.close()

def load_neighbors(neighbors_file):
    """
    Carrega a lista de peers a partir de um arquivo de vizinhos.
    Cada linha do arquivo representa um peer, exceto o próprio servidor.
    Inicializa cada peer com status OFFLINE e relógio 0.
    """
    if not os.path.isfile(neighbors_file):
        print(f"Arquivo de vizinhos {neighbors_file} não existe.")
        sys.exit(1)
        
    with open(neighbors_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and line != server_id:  # Ignora o próprio servidor
                with peers_lock:
                    if line not in peers:
                        peers[line] = {"status": "OFFLINE", "clock": 0}
                        print(f"Adicionando novo peer {line} status OFFLINE e relógio 0")

def list_local_files():
    """
    Lista e exibe os arquivos presentes no diretório compartilhado.
    """
    try:
        files = os.listdir(shared_dir)
        if not files:
            print("Nenhum arquivo no diretório compartilhado.")
        else:
            for filename in sorted(files):
                print(filename)
    except Exception as e:
        print(f"Erro ao listar arquivos: {e}")

def command_list_peers():
    """
    Permite ao usuário listar os peers conhecidos e enviar uma mensagem HELLO
    para atualizar seu status.
    """
    while True:
        with peers_lock:
            if not peers:
                print("Nenhum peer conhecido.")
                return
                
            print("\nLista de peers:")
            print("[0] Voltar para o menu anterior")
            peers_list = list(peers.items())
            for idx, (peer, info) in enumerate(peers_list, start=1):
                print(f"[{idx}] {peer} {info['status']} {info['clock']}")
                
        escolha = input("> ").strip()
        if escolha == "0":
            return
            
        try:
            idx = int(escolha) - 1
            if idx < 0 or idx >= len(peers_list):
                print("Opção inválida!")
                continue
                
            peer_addr, _ = peers_list[idx]
            # Envia mensagem HELLO para o peer selecionado
            message = "HELLO"
            reply = send_message(peer_addr, message, max_retries=2)

            new_status = "ONLINE" if reply is not None else "OFFLINE"

            with peers_lock:
                if peers[peer_addr]["status"] != new_status:
                    peers[peer_addr]["status"] = new_status
                    print(f"Atualizando peer {peer_addr} status {new_status}")
                    
        except ValueError:
            print("Digite um número válido1!")

def command_get_peers():
    """
    Envia uma solicitação GET_PEERS para cada peer conhecido, atualizando a lista
    com os peers retornados na resposta.
    """
    with peers_lock:
        current_peers = list(peers.keys())
        
    for peer_addr in current_peers:
        msg_clock = update_clock()
        message = f"GET_PEERS"
        reply = send_message(peer_addr, message, expect_reply=True)
        
        if reply:
            try:
                parts = reply.strip().split()
                if len(parts) >= 4 and parts[2] == "PEER_LIST":
                    total = int(parts[3])
                    new_peers = parts[4:4+total] if total > 0 else []
                    
                    if len(new_peers) != total:
                        print(f"Aviso: número de peers recebidos ({len(new_peers)}) difere do total informado ({total})")
                    
                    for peer_info in new_peers:
                        try:
                            peer_parts = peer_info.split(":")
                            if len(peer_parts) >= 3:
                                peer_id = f"{peer_parts[0]}:{peer_parts[1]}"
                                status = peer_parts[2]
                                clock = int(peer_parts[3])
                                
                                if peer_id == server_id:
                                    continue  # Ignora o próprio servidor
                                    
                                with peers_lock:
                                    if peer_id in peers:
                                        if peers[peer_id]["status"] != status or peers[peer_id]["clock"] != clock:
                                            peers[peer_id] = {"status": status, "clock": clock}
                                            print(f"Atualizando peer {peer_id} status {status} e relógio para {clock}")
                                    else:
                                        peers[peer_id] = {"status": status, "clock": clock}
                                        print(f"Adicionando novo peer {peer_id} status {status} e relógio para {clock}")
                        except Exception as e:
                            print(f"Erro ao processar peer {peer_info}: {e}")
                            
                    # Marca o peer que respondeu como ONLINE
                    with peers_lock:
                        if peers[peer_addr]["status"] != "ONLINE":
                            peers[peer_addr]["status"] = "ONLINE"
                            print(f"Atualizando peer {peer_addr} status ONLINE")
                else:
                    print(f"Resposta inválida de {peer_addr}: {reply}")
            except Exception as e:
                print(f"Erro ao processar resposta de {peer_addr}: {e}")
        else:
            with peers_lock:
                if peers[peer_addr]["status"] != "OFFLINE":
                    peers[peer_addr]["status"] = "OFFLINE"
                    print(f"Atualizando peer {peer_addr} status OFFLINE")

def command_list_local_files():
    """
    Exibe a lista dos arquivos presentes no diretório compartilhado.
    """
    print("\nArquivos no diretório compartilhado:")
    list_local_files()

def command_exit():
    """
    Envia mensagem BYE para todos os peers ONLINE e encerra o programa.
    """
    global running
    
    print("\nSaindo...")
    
    # Notifica todos os peers que estão ONLINE sobre o encerramento
    with peers_lock:
        online_peers = [peer for peer, info in peers.items() if info["status"] == "ONLINE"]
        
    for peer_addr in online_peers:
        message = "BYE"
        send_message(peer_addr, message)
        
    time.sleep(1)  # Aguarda envio das mensagens
    running = False
    if server_socket:
        try:
            server_socket.close()
        except:
            pass
    
    sys.exit(0)

def print_menu():
    """
    Exibe o menu interativo com as opções disponíveis para o usuário.
    """
    print("\n" + "="*50)
    print("Escolha um comando:")
    print("[1] Listar peers")
    print("[2] Obter peers")
    print("[3] Listar arquivos locais")
    print("[4] Buscar arquivos")
    print("[5] Exibir estatisticas")
    print("[6] Alterar tamanho de chunk")
    print("[9] Sair")
    print("="*50)

def menu():
    """
    Loop principal do menu interativo. Processa as escolhas do usuário.
    """
    while True:
        print_menu()
        escolha = input("> ").strip()
        
        if escolha == "1":
            command_list_peers()
        elif escolha == "2":
            command_get_peers()
        elif escolha == "3":
            command_list_local_files()
        elif escolha == "4":
            command_search_files()
        elif escolha == "5":
            print("\nComando 'Exibir estatisticas' não implementado nesta versão.")
        elif escolha == "6":
            print("\nComando 'Alterar tamanho de chunk' não implementado nesta versão.")
        elif escolha == "9":
            command_exit()
        else:
            print("\nOpção inválida! Tente novamente.")

def main():
    """
    Função principal que:
      - Valida os parâmetros de entrada;
      - Configura o servidor, vizinhos e diretório compartilhado;
      - Inicia o servidor em uma thread separada;
      - Inicia o menu interativo.
    """
    global server_id, local_ip, local_port, shared_dir

    if len(sys.argv) != 4:
        print("Uso: ./eachare <endereco:porta> <vizinhos.txt> <diretorio_compartilhado>")
        sys.exit(1)

    # Processa o parâmetro com o endereço e a porta
    id_param = sys.argv[1]
    if ":" not in id_param:
        print("Formato inválido para endereço:porta")
        sys.exit(1)
        
    local_ip, port_str = id_param.split(":")
    try:
        local_port = int(port_str)
    except ValueError:
        print("Porta inválida")
        sys.exit(1)
        
    server_id = f"{local_ip}:{local_port}"

    # Carrega a lista de peers a partir do arquivo de vizinhos
    neighbors_file = sys.argv[2]
    load_neighbors(neighbors_file)

    # Verifica se o diretório compartilhado é válido e acessível
    shared_dir = sys.argv[3]
    if not os.path.isdir(shared_dir):
        print("Diretório compartilhado não existe.")
        sys.exit(1)
    if not os.access(shared_dir, os.R_OK):
        print("Sem permissão de leitura no diretório compartilhado.")
        sys.exit(1)

    # Inicia o servidor TCP em uma thread separada
    threading.Thread(target=server_thread).start()

    time.sleep(0.5)  # Pausa para garantir que o servidor esteja ativo
    menu()

if __name__ == "__main__":
    main()