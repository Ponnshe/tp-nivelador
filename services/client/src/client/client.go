package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 10
const CONNECTION_ATTEMPS_DELAY_MS = 500

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn      net.Conn
	config    ClientConfig
	stopped   atomic.Bool
	stopChan  chan struct{}
	closeOnce sync.Once
}

func NewClient(config ClientConfig) *Client {
	return &Client{
		config:   config,
		stopChan: make(chan struct{}),
	}
}

func (c *Client) Stop() {
	c.closeOnce.Do(func() {
		c.stopped.Store(true)
		close(c.stopChan)
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

func (c *Client) connectToServer() error {
	const action = "connect-to-server"
	var err error

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		if c.stopped.Load() {
			return nil
		}

		conn, dialErr := net.Dial("tcp", c.config.ServerHost+":"+c.config.ServerPort)
		if dialErr == nil {
			c.conn = conn
			logger.Info(action, logger.Success)
			return nil
		}
		err = dialErr

		logger.Warn(action, logger.Fail, "attempt", i)
		select {
		case <-c.stopChan:
			return nil
		case <-time.After(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond):
		}
	}

	return err
}

func (c *Client) sendBatch(batch []string) error {
	if len(batch) == 0 || c.stopped.Load() {
		return nil
	}
	
	payload := []byte(strings.Join(batch, "\n") + "\n")
	if err := protocol.SendMsg(c.conn, protocol.MsgBet, payload); err != nil {
		if c.stopped.Load() { return nil }
		logger.Error("send-bet-batch", logger.Fail, "agency-id", c.config.AgencyId, "bets", len(batch), "err", err)
		return err
	}

	msgType, _, err := protocol.RecvMsg(c.conn)
	if err != nil {
		if c.stopped.Load() { return nil }
		logger.Error("recv-ack", logger.Fail, "agency-id", c.config.AgencyId, "err", err)
		return err
	}
	
	if msgType != protocol.MsgAck {
		if c.stopped.Load() { return nil }
		logger.Error("check-ack", logger.Fail, "agency-id", c.config.AgencyId, "expected", protocol.MsgAck, "got", msgType)
		return fmt.Errorf("unexpected message type: %d", msgType)
	}

	return nil
}

func (c *Client) processBets(inputFile *os.File) (int, error) {
	scanner := bufio.NewScanner(inputFile)
	var batch []string
	lineCount := 0

	for scanner.Scan() {
		if c.stopped.Load() {
			return lineCount, nil
		}
		
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}
		
		lineCount++
		batch = append(batch, c.config.AgencyId+","+line)

		if len(batch) >= c.config.BatchSize {
			if err := c.sendBatch(batch); err != nil {
				return lineCount, err
			}
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		if c.stopped.Load() { return lineCount, nil }
		logger.Error("scan-file", logger.Fail, "agency-id", c.config.AgencyId, "err", err)
		return lineCount, err
	}

	// Remanente
	if len(batch) > 0 {
		if err := c.sendBatch(batch); err != nil {
			return lineCount, err
		}
	}

	return lineCount, nil
}

func (c *Client) fetchAndWriteWinners(outputFile *os.File) error {
	if c.stopped.Load() {
		return nil
	}

	if err := protocol.SendMsg(c.conn, protocol.MsgEndBets, []byte(c.config.AgencyId)); err != nil {
		if c.stopped.Load() { return nil }
		logger.Error("send-end-bets", logger.Fail, "agency-id", c.config.AgencyId, "err", err)
		return err
	}

	msgType, winnersPayload, err := protocol.RecvMsg(c.conn)
	if err != nil {
		if c.stopped.Load() { return nil }
		logger.Error("recv-winners", logger.Fail, "agency-id", c.config.AgencyId, "err", err)
		return err
	}
	
	if msgType != protocol.MsgWinners {
		if c.stopped.Load() { return nil }
		logger.Error("check-winners", logger.Fail, "agency-id", c.config.AgencyId, "expected", protocol.MsgWinners, "got", msgType)
		return fmt.Errorf("unexpected message type: %d", msgType)
	}

	if len(winnersPayload) > 0 {
		if _, err := outputFile.Write(winnersPayload); err != nil {
			if c.stopped.Load() { return nil }
			logger.Error("write-output", logger.Fail, "agency-id", c.config.AgencyId, "err", err)
			return err
		}
	}

	return nil
}

func (c *Client) Run() error {
	const mainAction = "send-bets"

	if err := c.connectToServer(); err != nil {
		if c.stopped.Load() { return nil }
		logger.Warn("connect-to-server", logger.Fail)
		return err
	}
	if c.stopped.Load() { return nil }
	defer c.Stop()

	inputFile, err := os.Open(c.config.InputFile)
	if err != nil {
		if c.stopped.Load() { return nil }
		logger.Error("open-input-file", logger.Fail, "err", err, "path", c.config.InputFile)
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(c.config.OutputFile)
	if err != nil {
		if c.stopped.Load() { return nil }
		logger.Error("create-output-file", logger.Fail, "err", err, "path", c.config.OutputFile)
		return err
	}
	defer outputFile.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", c.config.AgencyId)

	lineCount, err := c.processBets(inputFile)
	if err != nil {
		return err
	}

	if err := c.fetchAndWriteWinners(outputFile); err != nil {
		return err
	}

	if !c.stopped.Load() {
		logger.Info(mainAction, logger.Success, "agency-id", c.config.AgencyId, "total-bets", lineCount)
	}
	
	return nil
}
