package nats_test

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/stewelarend/rpc"
	"github.com/stewelarend/rpc/server/nats"
	"github.com/stewelarend/util"
)

func TestOneRequest(t *testing.T) {
	s := newSession(t)
	defer s.End()

	//send one request:
	if double := s.Request(123); double != 246 {
		t.Fatalf("wrong testResponse: 123 -> %+v", double)
	}
}

func Test2(t *testing.T) {
	s := newSession(t)
	defer s.End()

	//send many requests:
	for i := 0; i < 100; i++ {
		if double := s.Request(i); double != i*2 {
			t.Fatalf("wrong testResponse: %d -> %+v", i, double)
		}
	}
}

type session struct {
	t      *testing.T
	server rpc.IServer
	topic  string
	conn   *gonats.Conn
}

const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func newSession(t *testing.T) session {
	s := session{
		t: t,
	}
	s.topic = ""
	for i := 0; i < 32; i++ {
		s.topic += string(chars[rand.Intn(len(chars))])
	}

	//connect to nats so we can send requests to the server
	uri := "nats://localhost:4222"
	var err error
	s.conn, err = gonats.Connect(uri)
	if err != nil {
		t.Fatalf("cannot connect to nats")
	}

	//create the server that will be tested
	service := rpc.New("test")
	service.AddStruct("test", testRequest{})
	s.server, err = nats.Config{
		URI:   uri,
		Topic: s.topic,
	}.Create(service, request{}, response{})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	go s.server.Run() //todo: stop the run when session ends...

	time.Sleep(time.Second) //wait so server can subscribe
	return s
} //newSession()

func (s session) End() {
	s.conn.Close()
}

func (s session) Request(value int) int {
	encodedRequest, _ := request{Oper: "test", OperData: testRequest{Value: value}}.Encode()
	natsRes, err := s.conn.Request(s.topic, encodedRequest, time.Second)
	if err != nil {
		s.t.Fatalf("failed to request: %v", err)
	}
	_res, err := util.StructDecode(response{}, natsRes.Data)
	res := _res.(response)
	testRes, err := util.StructFromValue(&testResponse{}, res.Data())
	if err != nil {
		s.t.Fatalf("failed to decode test response: %v", err)
	}
	return testRes.(*testResponse).Double
}

type testRequest struct {
	Value int `json:"value"`
	Delay int `json:"delay"` //nr of seconds
}

func (req testRequest) Exec(ctx rpc.IContext) (res interface{}, err error) {
	if time.Second > 0 {
		time.Sleep(time.Duration(req.Delay) * time.Second)
	}
	return testResponse{
		Double: req.Value * 2,
	}, nil
}

type testResponse struct {
	Double int `json:"double"`
}

//request implements rpc.IRequest for the request messages sent to the server
type request struct {
	Oper     string      `json:"operation"`
	OperData interface{} `json:"data"`
}

func (m request) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m request) Decode(data []byte) (util.IDecoder, error) {
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (m request) Operation() string {
	return m.Oper
}

func (m request) Data() interface{} {
	return m.OperData
}

//response implements rpc.IResponse for the response messages sent from the server
type response struct {
	OperData interface{} `json:"value"`
}

func (m response) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m response) Decode(data []byte) (util.IDecoder, error) {
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (m response) Data() interface{} {
	return m.OperData
}

func (m response) Success(data interface{}) rpc.IResponse {
	m.OperData = data
	return m
}

func (m response) Error(err error) rpc.IResponse {
	return m
}
