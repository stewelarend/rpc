package rpc

import "fmt"

type IServer interface {
	Run() error
}

type IServerConstructor interface {
	Validate() error
	Create(IService) (IServer, error)
}

var constructors = map[string]IServerConstructor{}

func RegisterServer(name string, constructor IServerConstructor) {
	constructors[name] = constructor
	fmt.Printf("Registered constructor[%s] = %+v\n", name, constructor)
}

func NewServer(service IService, name string, config map[string]interface{}) (IServer, error) {
	constructor, ok := constructors[name]
	if !ok {
		panic(fmt.Errorf("server(%s) constructor is not registered", name))
	}
	return constructor.Create(service)
}
