package client

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/subiz/header"
	pb "github.com/subiz/header/pubsub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
)

// Client helps you send message to realtime service easier
type Client struct {
	sync.Mutex
	clients []header.PubsubClient

	service  string // eg: realtime:48883
	maxNodes int
}

// NewClient creates a new Client service
// Notes: we don't connect to all realtime service right away because it
// may blocks start up flow
func NewClient(service string, maxNodes int) *Client {
	return &Client{
		service:  service,
		maxNodes: maxNodes,
		clients:  make([]header.PubsubClient, maxNodes, maxNodes),
	}
}

// Send delivers a payload to the correct realtime service
func (me *Client) Send(accid string, topics []string, payload []byte) error {
	if len(topics) == 0 {
		return nil
	}

	client, err := me.getPubsubClient(accid)
	if err != nil {
		return err
	}
	ctx := context.Background()

	_, err = client.Publish(ctx, &pb.PublishMessage{AccountId: accid, Payload: payload, Topics: topics})
	return err
}

// getPubsubClient returns the correct pubsubClient for an account ID
// the returned client must be ready to be used
func (me *Client) getPubsubClient(accid string) (header.PubsubClient, error) {
	no := int(crc32.ChecksumIEEE([]byte(accid))) % me.maxNodes

	me.Lock()
	if me.clients[no] != nil {
		me.Unlock()
		return me.clients[no], nil
	}

	defer me.Unlock()

	parts := strings.SplitN(me.service, ":", 2)
	name, port := parts[0], parts[1]
	// address: [pod name] + "." + [service name] + ":" + [pod port]
	conn, err := dialGrpc(name + "-" + strconv.Itoa(no) + "." + name + ":" + port)
	if err != nil {
		fmt.Println("unable to connect to pubsub service", err)
		return nil, err
	}
	me.clients[no] = header.NewPubsubClient(conn)
	return me.clients[no], nil
}

func dialGrpc(service string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	// Enabling WithBlock tells the client to not give up trying to find a server
	opts = append(opts, grpc.WithBlock())
	// However, we're still setting a timeout so that if the server takes too long, we still give up
	opts = append(opts, grpc.WithTimeout(10*time.Second))
	opts = append(opts, grpc.WithBalancerName(roundrobin.Name))
	return grpc.Dial(service, opts...)
}
