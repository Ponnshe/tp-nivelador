import os
import socket
import threading
import queue
import logger
import protocol
from lottery import Lottery, Bet


class Server:
    def __init__(
        self,
        server_host: str,
        server_port: int,
        agency_quorum_min: int = 1,
        storage_path: str = "bets.csv",
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.storage_path = storage_path
        if os.path.exists(self.storage_path):
            os.remove(self.storage_path)
        self.lottery = Lottery(self.storage_path)

        self.coordinator_queue = queue.Queue()
        self.is_running = True
        self.server_socket = None
        self.coordinator_thread = None
        self.client_threads = []
        self.client_sockets = set()
        self.lock = threading.Lock()

    def stop(self):
        if not self.is_running:
            return
        self.is_running = False

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        self.coordinator_queue.put(("STOP",))

        with self.lock:
            sockets_to_close = list(self.client_sockets)
            threads_to_join = list(self.client_threads)

        for s in sockets_to_close:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                s.close()
            except Exception:
                pass

        if self.coordinator_thread and self.coordinator_thread.is_alive():
            self.coordinator_thread.join(timeout=2.0)

        for t in threads_to_join:
            if t.is_alive():
                t.join(timeout=2.0)

    def _run_coordinator(self):
        """Hilo coordinador: dueño exclusivo del estado de agencias y de la lotería."""
        notified_agencies = set()
        pending_agencies = []
        quorum_reached = False
        winners_by_agency = {}

        while self.is_running:
            try:
                msg = self.coordinator_queue.get(timeout=1)
            except queue.Empty:
                continue

            if msg is None or msg[0] == "STOP":
                for _, reply_q in pending_agencies:
                    reply_q.put(None)
                break

            msg_type = msg[0]

            if msg_type == "STORE_BETS":
                _, bets, reply_q = msg
                try:
                    self.lottery.store_bets(bets)
                    reply_q.put(True)
                except Exception as e:
                    reply_q.put(e)

            elif msg_type == "END_BETS":
                _, agency_id, reply_q = msg
                notified_agencies.add(agency_id)

                if not quorum_reached and len(notified_agencies) >= self.agency_quorum_min:
                    quorum_reached = True
                    pending_agencies.append((agency_id, reply_q))
                    winners_map = {}
                    for bet in self.lottery.load_bets():
                        if self.lottery.has_won(bet):
                            winners_map.setdefault(bet.agency_id, []).append(
                                f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
                            )
                    winners_by_agency = {}
                    for aid, lines in winners_map.items():
                        winners_by_agency[aid] = "\n".join(lines).encode("utf-8") + b"\n"

                    for aid, q in pending_agencies:
                        q.put(winners_by_agency.get(aid, b""))
                    pending_agencies.clear()

                elif not quorum_reached:
                    pending_agencies.append((agency_id, reply_q))

                else:
                    reply_q.put(winners_by_agency.get(agency_id, b""))

            elif msg_type == "DISCONNECT":
                _, reply_q = msg
                pending_agencies = [
                    (aid, q) for aid, q in pending_agencies if q != reply_q
                ]

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        reply_queue = queue.Queue(maxsize=1)
        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.is_running:
                msg_type, payload = protocol.recv_msg(client_socket)
                if msg_type is None:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    return

                if msg_type == protocol.MSG_BET:
                    text = payload.decode("utf-8")
                    bets = []
                    for line in text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(",")
                        agency_id = int(parts[0])
                        first_name = parts[1]
                        last_name = parts[2]
                        document = int(parts[3])
                        birthdate = parts[4]
                        number = int(parts[5])
                        bets.append(
                            Bet(
                                agency_id,
                                first_name,
                                last_name,
                                document,
                                birthdate,
                                number,
                            )
                        )
                    if bets:
                        self.coordinator_queue.put(("STORE_BETS", bets, reply_queue))
                        res = reply_queue.get()
                        if isinstance(res, Exception):
                            raise res
                        if res is None:
                            return
                        message_amount += len(bets)
                    protocol.send_msg(client_socket, protocol.MSG_ACK)

                elif msg_type == protocol.MSG_END_BETS:
                    agency_id = int(payload.decode("utf-8").strip())
                    self.coordinator_queue.put(("END_BETS", agency_id, reply_queue))
                    winners_payload = reply_queue.get()
                    if winners_payload is not None:
                        protocol.send_msg(
                            client_socket, protocol.MSG_WINNERS, winners_payload
                        )
                    return

                else:
                    logger.warn("unknown-msg-type", logger.LogResult.fail, "type", msg_type)

        except Exception as e:
            if self.is_running:
                logger.error(
                    action, logger.LogResult.fail, "messages-amount", message_amount, "err", e
                )
        finally:
            self.coordinator_queue.put(("DISCONNECT", reply_queue))
            with self.lock:
                self.client_sockets.discard(client_socket)
            try:
                client_socket.close()
            except Exception:
                pass

    def run(self):
        action = "accept-connection"
        self.coordinator_thread = threading.Thread(target=self._run_coordinator, daemon=True)
        self.coordinator_thread.start()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_host, self.server_port))
        self.server_socket.listen()

        try:
            while self.is_running:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = self.server_socket.accept()
                except (OSError, Exception) as e:
                    if not self.is_running:
                        break
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                with self.lock:
                    self.client_sockets.add(client_socket)

                client_thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                )
                with self.lock:
                    self.client_threads.append(client_thread)
                client_thread.start()
        finally:
            self.stop()
