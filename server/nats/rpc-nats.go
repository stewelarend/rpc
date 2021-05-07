package nats

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stewelarend/consumer/message"
	"github.com/stewelarend/logger"
	"github.com/stewelarend/rpc"
	"github.com/stewelarend/util"
)

var log = logger.New("nats-server")

func init() {
	rpc.RegisterServer(
		"nats",
		&config{
			URI:       "nats://nats.4222",
			Topic:     "", //must be configured
			NrWorkers: 1,
		},
	)
}

type config struct {
	URI       string `json:"uri" doc:"NATS URI (default: nats://nats:4222)"`
	Topic     string `json:"topic" doc:"Topic to process requests from"`
	NrWorkers int    `json:"nr_workers" doc:"Control number of workers for parallel processing (default: 1)"`
}

func (c *config) Validate() error {
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

func (c config) Create(service rpc.IService) (rpc.IServer, error) {
	fmt.Printf("Create NATS server: %+v\n", c)
	return &server{
		service: service,
		config:  c,
	}, nil
}

type server struct {
	service rpc.IService
	config  config
	nc      *nats.Conn
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
		for {
			log.Debugf("loop")
			select {
			case msg := <-msgChan:
				log.Debugf("Got message(sub:%s,reply:%s,hdr:%+v),data(%s)", msg.Subject, msg.Reply, msg.Header, string(msg.Data))
				wg.Add(1)
				go func(msg *nats.Msg) {
					s.handle(msg)
					wg.Done()
				}(msg)
			case <-time.After(time.Second):
				log.Debugf("no messages yet...")
			}
		} //for main loop
	}()

	defer func() {
		close(msgChan)
		log.Debugf("Closed chan")
		wg.Wait()
		log.Debugf("Workers stopped")
	}()

	//subscribe to write into the message channel
	subscription, err := s.nc.QueueSubscribeSyncWithChan(
		s.config.Topic,
		s.config.Topic, //Group,
		msgChan)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %v", err)
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
	var request message.Request

	//prepare response
	response := message.Response{
		Message: message.Message{
			Timestamp: time.Now(),
		},
		Result: []message.Result{{
			Timestamp: time.Now(), //updated after parsing the request
			StartTime: time.Now(),
			Duration:  0, //set when responding
			Service:   s.service.Name(),
			Operation: "",        //set after parsing the request
			Code:      -1,        //set when responding
			Name:      "Unknown", //set when responding
			Details:   "",
		}},
		Data: nil,
	}
	defer func() {
		if msg.Reply == "" {
			log.Errorf("sender did not specify NATS reply topic for response: req=%+v, res=%+v", request, response)
		}
		response.Result[0].Duration = time.Now().Sub(response.Result[0].StartTime)
		jsonResponse, _ := json.Marshal(response)
		if err := s.nc.Publish(msg.Reply, jsonResponse); err != nil {
			log.Errorf("failed to send error response to reply topic: %v", err)
		}
	}()

	if err := json.Unmarshal(msg.Data, &request); err != nil {
		response.Result[0].Code = -1               //todo: use predefined
		response.Result[0].Name = "InvalidRequest" //todo: use predefined
		response.Result[0].Details = fmt.Sprintf("failed to decode JSON request: %v", err)
		return
	}
	if err := request.Validate(); err != nil {
		response.Result[0].Code = -1               //todo: use predefined
		response.Result[0].Name = "InvalidRequest" //todo: use predefined
		response.Result[0].Details = fmt.Sprintf("invalid request: %v", err)
		return
	}
	log.Debugf("Valid Request: %+v", request)
	response.Result[0].Timestamp = request.Timestamp
	response.Result[0].Operation = request.Operation

	//lookup operation
	oper, ok := s.service.Oper(request.Operation)
	if !ok {
		response.Result[0].Code = -1            //todo: use predefined
		response.Result[0].Name = "UnknownType" //todo: use predefined
		response.Result[0].Details = fmt.Sprintf("unknown request type(%s)", request.Operation)
		return
	}

	//parse the event.Request to create a populated oper struct
	newOper, err := util.StructFromValue(oper, request.Data)
	if err != nil {
		response.Result[0].Code = -1               //todo: use predefined
		response.Result[0].Name = "InvalidRequest" //todo: use predefined
		response.Result[0].Details = fmt.Sprintf("cannot parse %s request data into %T: %v", request.Operation, oper, err)
		return
	}
	oper = newOper.(rpc.IHandler)
	if validator, ok := oper.(IValidator); ok {
		if err := validator.Validate(); err != nil {
			log.Errorf("invalid %s event data: %v", request.Operation, err)
			return
		}
	}

	//process the parsed request
	operResponse, err := oper.Exec(nil)
	if err != nil {
		//todo: get []result from operation..., e.g. if called other services
		//then add another result before that in the response

		response.Result[0].Code = -1           //todo: use predefined
		response.Result[0].Name = "OperFailed" //todo: use predefined
		response.Result[0].Details = fmt.Sprintf("oper(%s) failed: %v", request.Operation, err)
		return
	}

	log.Debugf("oper(%s) success", request.Operation)
	response.Result[0].Code = 0         //todo: use predefined
	response.Result[0].Name = "success" //todo: use predefined
	response.Result[0].Details = ""
	response.Data = operResponse
}

type IValidator interface {
	Validate() error
}
