package net

import (
	"context"
	"fmt"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	libpeer "github.com/libp2p/go-libp2p-core/peer"
	rpc "github.com/textileio/go-libp2p-pubsub-rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcpeer "google.golang.org/grpc/peer"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/document/key"
	pb "github.com/sourcenetwork/defradb/net/pb"
)

// Server is the request/response instance for all P2P RPC communication.
// Implements gRPC server. See net/pb/net.proto for corresponding service definitions.
//
// Specifically, server handles the push/get request/response aspects of the RPC service
// but not the API calls.
type server struct {
	peer *peer
	opts []grpc.DialOption
	db   client.DB

	topics map[key.DocKey]*rpc.Topic

	conns map[libpeer.ID]*grpc.ClientConn

	sync.Mutex
}

// newServer creates a new network server that handle/directs RPC requests to the
// underlying DB instance.
func newServer(p *peer, db client.DB, opts ...grpc.DialOption) (*server, error) {
	s := &server{
		peer:   p,
		conns:  make(map[libpeer.ID]*grpc.ClientConn),
		topics: make(map[key.DocKey]*rpc.Topic),
	}

	defaultOpts := []grpc.DialOption{
		s.getLibp2pDialer(),
		grpc.WithInsecure(),
	}

	s.opts = append(defaultOpts, opts...)
	if s.peer.ps != nil {
		var keys []key.DocKey // @todo: Get all DocKeys across all collections in the DB
		for _, key := range keys {
			if err := s.addPubSubTopic(key); err != nil {
				return nil, err
			}
		}

	}

	return s, nil
}

// GetDocGraph recieves a get graph request
func (s *server) GetDocGraph(ctx context.Context, req *pb.GetDocGraphRequest) (*pb.GetDocGraphReply, error)

// PushDocGraph recieves a push graph request
func (s *server) PushDocGraph(ctx context.Context, req *pb.PushDocGraphRequest) (*pb.PushDocGraphReply, error)

// GetLog recieves a get log request
func (s *server) GetLog(ctx context.Context, req *pb.GetLogRequest) (*pb.GetLogReply, error)

// PushLog recieves a push log request
func (s *server) PushLog(ctx context.Context, req *pb.PushLogRequest) (*pb.PushLogReply, error)

// GetHeadLog recieves a get head log request
func (s *server) GetHeadLog(ctx context.Context, req *pb.GetHeadLogRequest) (*pb.GetHeadLogReply, error)

// addPubSubTopic subscribes to a DocKey topic
func (s *server) addPubSubTopic(dockey key.DocKey) error {
	if s.peer.ps == nil {
		return nil
	}

	s.Lock()
	defer s.Unlock()
	if _, ok := s.topics[dockey]; ok {
		return nil
	}

	t, err := rpc.NewTopic(s.peer.ctx, s.peer.ps, s.peer.host.ID(), dockey.String(), true)
	if err != nil {
		return err
	}

	t.SetEventHandler(s.pubSubEventHandler)
	t.SetMessageHandler(s.pubSubMessageHandler)
	s.topics[dockey] = t
	return nil
}

// removePubSubTopic unsubscribes to a DocKey topic
func (s *server) removePubSubTopic(dockey key.DocKey) error {
	if s.peer.ps == nil {
		return nil
	}

	s.Lock()
	defer s.Unlock()
	if t, ok := s.topics[dockey]; ok {
		delete(s.topics, dockey)
		return t.Close()
	}
	return nil
}

// pubSubMessageHandler handles incoming PushLog messages from the pubsub network.
func (s *server) pubSubMessageHandler(from libpeer.ID, topic string, msg []byte) ([]byte, error) {
	req := new(pb.PushLogRequest)
	if err := proto.Unmarshal(msg, req); err != nil {
		return nil, err
	}

	ctx := grpcpeer.NewContext(s.peer.ctx, &grpcpeer.Peer{
		Addr: addr{from},
	})
	if _, err := s.PushLog(ctx, req); status.Codes(err) == codes.NotFound {
		// log err
	} else if err != nil {
		return nil, fmt.Errorf("failed pushing log to doc %s: %w", topic, err)
	}
	return nil, nil
}

// pubSubEventHandler logs events from the subscribed dockey topics.
func (s *server) pubSubEventHandler(from libpeer.ID, topic string, msg []byte) {}

// addr implements net.Addr and holds a libp2p peer ID.
type addr struct{ id libpeer.ID }

// Network returns the name of the network that this address belongs to (libp2p).
func (a addr) Network() string { return "libp2p" }

// String returns the peer ID of this address in string form (B58-encoded).
func (a addr) String() string { return a.id.Pretty() }
