import os
import socket
import threading
import queue
import logger
import protocol
from lottery import Lottery, Bet
from dataclasses import dataclass, field

@dataclass
class _CoordState:
    quorum_min: int
    notified: set = field(default_factory=set)
    pending: list = field(default_factory=list)
    quorum_reached: bool = False
    winners_by_agency: dict = field(default_factory=dict)


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
        state = _CoordState(self.agency_quorum_min)

        while self.is_running:
            try:
                msg = self.coordinator_queue.get(timeout=1)
            except queue.Empty:
                continue

            if msg is None or msg[0] == "STOP":
                for _, reply_q in state.pending:
                    reply_q.put(None)
                break

            msg_type = msg[0]
            if msg_type == "STORE_BETS":
                self._coord_store_bets(msg)
            elif msg_type == "END_BETS":
                self._coord_end_bets(msg, state)
            elif msg_type == "DISCONNECT":
                self._coord_disconnect(msg, state)

    def _coord_store_bets(self, msg):
        _, bets, reply_q = msg
        try:
            self.lottery.store_bets(bets)
            reply_q.put(True)
        except Exception as e:
            reply_q.put(e)

    def _coord_end_bets(self, msg, state: _CoordState):
        _, agency_id, reply_q = msg
        state.notified.add(agency_id)

        if not state.quorum_reached and len(state.notified) >= state.quorum_min:
            state.quorum_reached = True
            state.pending.append((agency_id, reply_q))
            
            winners_map = {}
            for bet in self.lottery.load_bets():
                if self.lottery.has_won(bet):
                    winners_map.setdefault(bet.agency_id, []).append(
                        f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
                    )
            
            for aid, lines in winners_map.items():
                state.winners_by_agency[aid] = "\n".join(lines).encode("utf-8") + b"\n"

            for aid, q in state.pending:
                q.put(state.winners_by_agency.get(aid, b""))
            state.pending.clear()

        elif not state.quorum_reached:
            state.pending.append((agency_id, reply_q))
        else:
            reply_q.put(state.winners_by_agency.get(agency_id, b""))

    def _coord_disconnect(self, msg, state: _CoordState):
        _, reply_q = msg
        state.pending = [(aid, q) for aid, q in state.pending if q != reply_q]

    def _parse_bets(self, payload: bytes) -> list[Bet]:
        text = payload.decode("utf-8")
        bets = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            bets.append(
                Bet(
                    int(parts[0]), parts[1], parts[2],
                    int(parts[3]), parts[4], int(parts[5])
                )
            )
        return bets

    def _process_msg_bet(self, client_socket, payload, reply_queue) -> tuple[int, bool]:
        bets = self._parse_bets(payload)
        if not bets:
            return 0, True
            
        self.coordinator_queue.put(("STORE_BETS", bets, reply_queue))
        res = reply_queue.get()
        
        if isinstance(res, Exception):
            raise res
        if res is None:
            return 0, False
            
        protocol.send_msg(client_socket, protocol.MSG_ACK)
        return len(bets), True

    def _process_msg_end_bets(self, client_socket, payload, reply_queue):
        agency_id = int(payload.decode("utf-8").strip())
        self.coordinator_queue.put(("END_BETS", agency_id, reply_queue))
        winners_payload = reply_queue.get()
        if winners_payload is not None:
            protocol.send_msg(client_socket, protocol.MSG_WINNERS, winners_payload)

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        reply_queue = queue.Queue(maxsize=1)
        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.is_running:
                msg_type, payload = protocol.recv_msg(client_socket)
                
                if msg_type is None:
                    logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
                    return

                if msg_type == protocol.MSG_BET:
                    count, continue_running = self._process_msg_bet(client_socket, payload, reply_queue)
                    message_amount += count
                    if not continue_running:
                        return

                elif msg_type == protocol.MSG_END_BETS:
                    self._process_msg_end_bets(client_socket, payload, reply_queue)
                    return

                else:
                    logger.warn("unknown-msg-type", logger.LogResult.fail, "type", msg_type)

        except Exception as e:
            if self.is_running:
                logger.error(action, logger.LogResult.fail, "messages-amount", message_amount, "err", e)
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
