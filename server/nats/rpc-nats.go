package nats

import (
	"fmt"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stewelarend/logger"
	"github.com/stewelarend/rpc"
	"github.com/stewelarend/util"
)

var log = logger.New("nats-server").WithLevel(logger.LevelError)

func init() {
	rpc.RegisterServer(
		"nats",
		&Config{
			URI:       "nats://nats.4222",
			Topic:     "", //must be configured
			NrWorkers: 1,
		},
	)
}

type Config struct {
	URI       string `json:"uri" doc:"NATS URI (default: nats://nats:4222)"`
	Topic     string `json:"topic" doc:"Topic to process requests from"`
	NrWorkers int    `json:"nr_workers" doc:"Control number of workers for parallel processing (default: 1)"`
}

func (c *Config) Validate() error {
	if c.URI == "" {
		c.URI = "nats://nats:4222"
	}
	if c.Topic == "" {
		return fmt.Errorf("missing topic")
	}
	if c.NrWorkers == 0 {
		c.NrWorkers = 1
	}
	if c.NrWorkers < 1 {
		return fmt.Errorf("nr_workers:%d must be >= 1", c.NrWorkers)
	}
	return nil
}

func (c Config) Create(service rpc.IService, tmplRequest rpc.IRequest, tmplResponse rpc.IResponse) (rpc.IServer, error) {
	if service == nil {
		return nil, fmt.Errorf("Create(service==nil)")
	}
	return &server{
		service:      service,
		config:       c,
		tmplRequest:  tmplRequest,
		tmplResponse: tmplResponse,
	}, nil
}

type server struct {
	service      rpc.IService
	config       Config
	tmplRequest  rpc.IRequest
	tmplResponse rpc.IResponse
	nc           *nats.Conn
}

func (s server) Run() error {
	var err error
	for i := 0; i < 5; i++ {
		s.nc, err = nats.Connect(s.config.URI)
		if err == nil {
			break
		}
		fmt.Println("Waiting before connecting to NATS at:", s.config.URI)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to NATS(%s): %v", s.config.URI, err)
	}
	log.Debugf("Connected to NATS(%s)", s.nc.ConnectedUrl())
	defer func() {
		s.nc.Close()
		s.nc = nil
	}()

	//create the message channel and start the workers
	msgChan := make(chan *nats.Msg, s.config.NrWorkers)
	wg := sync.WaitGroup{}
	go func() {
		for msg := range msgChan {
			if msg == nil {
				log.Debugf("msg==nil")
			} else {
				log.Debugf("Got message(sub:%s,reply:%s,hdr:%+v),data(%s)", msg.Subject, msg.Reply, msg.Header, string(msg.Data))
				wg.Add(1)
				go func(msg *nats.Msg) {
					s.handle(msg)
					wg.Done()
				}(msg)
			}
		} //for main loop
		log.Debugf("Out of main loop")
	}()

	defer func() {
		close(msgChan)
		log.Debugf("Closed chan")
		wg.Wait()
		log.Debugf("Workers stopped")
	}()

	//subscribe to write into the message channel
	log.Debugf("1")
	subscription, err := s.nc.QueueSubscribeSyncWithChan(
		s.config.Topic,
		s.config.Topic, //Group,
		msgChan)
	if err != nil {
		log.Debugf("1: %v", err)
		return fmt.Errorf("failed to subscribe to topic(%s): %v", s.config.Topic, err)
	}

	log.Debugf("Subscribed to '%s' for processing requests...", s.config.Topic)

	//wait for interrupt then unsubscribe and wait for all workers to terminate
	//todo...
	stop := make(chan bool)
	<-stop
	log.Debugf("stopping")

	if err := subscription.Unsubscribe(); err != nil {
		return fmt.Errorf("failed to unsubscribe: %v", err)
	}
	log.Debugf("Unsubscribed")
	return nil
}

func (s server) handle(msg *nats.Msg) {
	log.Debugf("Got request on: %s", msg.Subject)
	var request rpc.IRequest
	var response rpc.IResponse

	//prepare response
	// response := message.Response{
	// 	Message: message.Message{
	// 		Timestamp: time.Now(),
	// 	},
	// 	Result: []message.Result{{
	// 		Timestamp: time.Now(), //updated after parsing the request
	// 		StartTime: time.Now(),
	// 		Duration:  0, //set when responding
	// 		Service:   s.service.Name(),
	// 		Operation: "",        //set after parsing the request
	// 		Code:      -1,        //set when responding
	// 		Name:      "Unknown", //set when responding
	// 		Details:   "",
	// 	}},
	// 	Data: nil,
	// }
	defer func() {
		if msg.Reply == "" {
			log.Errorf("sender did not specify NATS reply topic for response: req=%+v, res=%+v", request, response)
		}
		//response.Result[0].Duration = time.Now().Sub(response.Result[0].StartTime)
		encodedResponse, _ := response.Encode()
		if err := s.nc.Publish(msg.Reply, encodedResponse); err != nil {
			log.Errorf("failed to send error response to reply topic: %v", err)
		}
	}()

	reqStruct, err := util.StructDecode(s.tmplRequest, msg.Data)
	if err != nil {
		response = s.tmplResponse.Error(fmt.Errorf("failed to decode request: %v", err))
		return
	}
	request, ok := reqStruct.(rpc.IRequest)
	if !ok {
		response = s.tmplResponse.Error(fmt.Errorf("decoded reqStruct %T is not IRequest", reqStruct))
		return
	}

	if validator, ok := request.(rpc.IValidator); ok {
		if err := validator.Validate(); err != nil {
			response = s.tmplResponse.Error(fmt.Errorf("invalid request: %v", err))
			return
		}
	}

	log.Debugf("Valid Request: %+v", request)
	// response.Result[0].Timestamp = request.Timestamp
	// response.Result[0].Operation = request.Operation

	//lookup operation
	oper, ok := s.service.Oper(request.Operation())
	if !ok {
		response = s.tmplResponse.Error(fmt.Errorf("unknown operation(%s)", request.Operation()))
		return
	}

	//parse the request data to create a populated oper struct
	newOper, err := util.StructFromValue(oper, request.Data())
	if err != nil {
		response = s.tmplResponse.Error(fmt.Errorf("cannot parse %s request data into %T: %v", request.Operation(), oper, err))
		return
	}
	oper = newOper.(rpc.IHandler)
	if validator, ok := oper.(IValidator); ok {
		if err := validator.Validate(); err != nil {
			response = s.tmplResponse.Error(fmt.Errorf("invalid %s event data: %v", request.Operation(), err))
			return
		}
	}

	//process the parsed request
	operResponse, err := oper.Exec(nil)
	if err != nil {
		//todo: get []result from operation..., e.g. if called other services
		//then add another result before that in the response
		response = s.tmplResponse.Error(err)
		log.Errorf("oper(%s) failed: %v", request.Operation, err)
		return
	}

	log.Debugf("oper(%s) success", request.Operation())
	log.Debugf("respomse: %v", operResponse)
	response = s.tmplResponse.Success(operResponse)
}

type IValidator interface {
	Validate() error
}
