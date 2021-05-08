package rpc

import (
	"fmt"

	"github.com/stewelarend/logger"
	"github.com/stewelarend/util"
)

var log = logger.New("nats-server").WithLevel(logger.LevelError)

type IServer interface {
	Run() error
}

type IServerConstructor interface {
	Validate() error
	Create(service IService, tmplRequest IRequest, tmplResponse IResponse) (IServer, error)
}

type IRequest interface {
	util.IDecoder
	Encode() ([]byte, error)
	Operation() string
	Data() interface{}
}

type IResponse interface {
	util.IDecoder
	Encode() ([]byte, error)
	Data() interface{}

	Success(data interface{}) IResponse
	Error(err error) IResponse
}

var constructors = map[string]IServerConstructor{}

func RegisterServer(name string, constructor IServerConstructor) {
	constructors[name] = constructor
	log.Debugf("Registered constructor[%s] = %+v\n", name, constructor)
}

func NewServer(service IService, name string, config map[string]interface{}, tmplRequest IRequest, tmplResponse IResponse) (IServer, error) {
	constructor, ok := constructors[name]
	if !ok {
		panic(fmt.Errorf("server(%s) constructor is not registered", name))
	}
	return constructor.Create(service, tmplRequest, tmplResponse)
}
