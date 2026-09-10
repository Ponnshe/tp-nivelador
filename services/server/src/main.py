import os
import signal
import sys

import logger
import server

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
AGENCY_QUORUM_MIN = int(os.environ.get("AGENCY_QUORUM_MIN", 1))


def main():
    logger.init()
    s = server.Server(
        SERVER_HOST, SERVER_PORT, agency_quorum_min=AGENCY_QUORUM_MIN
    )

    def handle_signal(signum, frame):
        logger.info("server-signal", logger.LogResult.in_progress, "signal", signum)
        s.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        s.run()
    except Exception as e:
        logger.error("server-run", logger.LogResult.fail, "err", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
