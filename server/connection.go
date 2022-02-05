package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jeffjenkins/dispatchd/amqp"
	"github.com/jeffjenkins/dispatchd/stats"
	"github.com/jeffjenkins/dispatchd/util"
	"net"
	"sync"
	"time"
)

// TODO: we can only be "in" one of these at once, so this should probably
// be one field
type ConnectStatus struct {
	start    bool
	startOk  bool
	secure   bool
	secureOk bool
	tune     bool
	tuneOk   bool
	open     bool
	openOk   bool
	closing  bool
	closed   bool
}

type AMQPConnection struct {
	id                       int64
	nextChannel              int
	channels                 map[uint16]*Channel
	outgoing                 chan *amqp.WireFrame
	connectStatus            ConnectStatus
	server                   *Server
	network                  net.Conn
	lock                     sync.Mutex
	ttl                      time.Time
	sendHeartbeatInterval    time.Duration
	receiveHeartbeatInterval time.Duration
	maxChannels              uint16
	maxFrameSize             uint32
	clientProperties         *amqp.Table
	// stats
	statOutBlocked stats.Histogram
	statOutNetwork stats.Histogram
	statInBlocked  stats.Histogram
	statInNetwork  stats.Histogram
}

func (conn *AMQPConnection) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"id":               conn.id,
		"address":          fmt.Sprintf("%s", conn.network.RemoteAddr()),
		"clientProperties": conn.clientProperties.Table,
		"channelCount":     len(conn.channels),
	})
}

func NewAMQPConnection(server *Server, network net.Conn) *AMQPConnection {
	return &AMQPConnection{
		// If outgoing has a buffer the server performs better. I'm not adding one
		// in until I fully understand why that is
		id:                       util.NextId(),
		network:                  network,
		channels:                 make(map[uint16]*Channel),
		outgoing:                 make(chan *amqp.WireFrame, 100),
		connectStatus:            ConnectStatus{},
		server:                   server,
		receiveHeartbeatInterval: 60 * time.Second,
		maxChannels:              4096,
		maxFrameSize:             65536,
		// stats
		statOutBlocked: stats.MakeHistogram("Connection.Out.Blocked"),
		statOutNetwork: stats.MakeHistogram("Connection.Out.Network"),
		statInBlocked:  stats.MakeHistogram("Connection.In.Blocked"),
		statInNetwork:  stats.MakeHistogram("Connection.In.Network"),
	}
}

func (conn *AMQPConnection) openConnection() {
	// Negotiate Protocol
	buf := make([]byte, 8)
	_, err := conn.network.Read(buf)
	if err != nil {
		//fmt.Println("network error", conn.id)
		conn.hardClose()
		return
	}

	var supported = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	var supported2 = []byte{'A', 'M', 'Q', 'P', 1, 1, 0, 9} //some clients may declare this, like py-amqp<=1.4.9

	if bytes.Compare(buf, supported) != 0 {
		if bytes.Compare(buf, supported2) != 0 {
			//fmt.Println("not supported version:", string(buf), conn.id)
			conn.network.Write(supported)
			conn.hardClose()
			return
		}
	}

	// Create channel 0 and start the connection handshake
	conn.channels[0] = NewChannel(0, conn)
	conn.channels[0].start()
	conn.handleOutgoing()
	conn.handleIncoming()
}

func (conn *AMQPConnection) cleanUp() {

}

func (conn *AMQPConnection) deregisterChannel(id uint16) {
	delete(conn.channels, id)
}

func (conn *AMQPConnection) hardClose() {
	conn.network.Close()
	conn.connectStatus.closed = true
	conn.server.deregisterConnection(conn.id)
	conn.server.deleteQueuesForConn(conn.id)
	for _, channel := range conn.channels {
		channel.shutdown()
	}
}

func (conn *AMQPConnection) setMaxChannels(max uint16) {
	conn.maxChannels = max
}

func (conn *AMQPConnection) setMaxFrameSize(max uint32) {
	conn.maxFrameSize = max
}

func (conn *AMQPConnection) startSendHeartbeat(interval time.Duration) {
	conn.sendHeartbeatInterval = interval
	conn.handleSendHeartbeat()
}

func (conn *AMQPConnection) handleSendHeartbeat() {
	go func() {
		for {
			if conn.connectStatus.closed {
				break
			}
			time.Sleep(conn.sendHeartbeatInterval / 2)
			conn.outgoing <- &amqp.WireFrame{FrameType: 8, Channel: 0, Payload: make([]byte, 0)}
		}
	}()
}

func (conn *AMQPConnection) handleClientHeartbeatTimeout() {
	// TODO(MUST): The spec is that any octet is a heartbeat substitute. Right
	// now this is only looking at frames, so a long send could cause a timeout
	// TODO(MUST): if the client isn't heartbeating how do we know when it's
	// gone?
	go func() {
		for {
			if conn.connectStatus.closed {
				break
			}
			time.Sleep(conn.receiveHeartbeatInterval / 2) //
			// If now is higher than TTL we need to time the client out
			if conn.ttl.Before(time.Now()) {
				//fmt.Println("heartbeat timeout", conn.network.RemoteAddr())
				conn.hardClose()
			}
		}
	}()
}

func (conn *AMQPConnection) handleOutgoing() {
	// TODO(MUST): Use SetWriteDeadline so we never wait too long. It should be
	// higher than the heartbeat in use. It should be reset after the heartbeat
	// interval is known.
	go func() {
		for {
			if conn.connectStatus.closed {
				break
			}
			var start = stats.Start()
			var frame = <-conn.outgoing
			stats.RecordHisto(conn.statOutBlocked, start)

			// fmt.Printf("Sending outgoing message. type: %d\n", frame.FrameType)
			// TODO(MUST): Hard close on irrecoverable errors, retry on recoverable
			// ones some number of times.
			start = stats.Start()
			amqp.WriteFrame(conn.network, frame)
			stats.RecordHisto(conn.statOutNetwork, start)
			// for wire protocol debugging:
			// for _, b := range frame.Payload {
			// 	fmt.Printf("%d,", b)
			// }
			// fmt.Printf("\n")
		}
	}()
}

func (conn *AMQPConnection) connectionErrorWithMethod(amqpErr *amqp.AMQPError) {
	//fmt.Println("Sending connection error:", amqpErr.Msg)
	conn.connectStatus.closing = true
	conn.channels[0].SendMethod(&amqp.ConnectionClose{
		ReplyCode: amqpErr.Code,
		ReplyText: amqpErr.Msg,
		ClassId:   amqpErr.Class,
		MethodId:  amqpErr.Method,
	})
}

func (conn *AMQPConnection) handleIncoming() {
	for {
		// If the connection is done, we stop handling frames
		if conn.connectStatus.closed {
			break
		}
		// Read from the network
		// TODO(MUST): Add a timeout to the read, esp. if there is no heartbeat
		// TODO(MUST): Hard close on unrecoverable errors, retry (with backoff?)
		// for recoverable ones
		var start = stats.Start()
		frame, err := amqp.ReadFrame(conn.network)
		if err != nil {
			//fmt.Println("Error reading frame: " + err.Error())
			conn.hardClose()
			break
		}
		stats.RecordHisto(conn.statInNetwork, start)
		conn.handleFrame(frame)
	}
}

func (conn *AMQPConnection) handleFrame(frame *amqp.WireFrame) {

	// Upkeep. Remove things which have expired, etc
	conn.cleanUp()
	conn.ttl = time.Now().Add(conn.receiveHeartbeatInterval * 2)

	switch {
	case frame.FrameType == 8:
		// TODO(MUST): Update last heartbeat time
		return
	}

	if !conn.connectStatus.open && frame.Channel != 0 {
		//fmt.Println("Non-0 channel for unopened connection")
		conn.hardClose()
		return
	}
	var channel, ok = conn.channels[frame.Channel]
	// TODO(MUST): Check that the channel number if in the valid range
	if !ok {
		channel = NewChannel(frame.Channel, conn)
		conn.channels[frame.Channel] = channel
		conn.channels[frame.Channel].start()
	}
	// Dispatch
	start := stats.Start()
	channel.incoming <- frame
	stats.RecordHisto(conn.statInBlocked, start)
}
