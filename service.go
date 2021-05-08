package rpc

import (
	"fmt"
	"sort"

	"github.com/stewelarend/config"
)

func New(name string) IService {
	return service{
		name:     name,
		handlers: map[string]IHandler{},
	}
}

type IService interface {
	Name() string
	AddStruct(name string, handler IHandler)
	AddFunc(name string, handler HandlerFunc)
	Oper(name string) (IHandler, bool)
	Opers() []string
	Run(tmplRequest IRequest, tmplResponse IResponse) error
}

type HandlerFunc func(IContext, interface{}) (interface{}, error)

type IHandler interface {
	Exec(ctx IContext) (res interface{}, err error)
}

type IValidator interface {
	Validate() error
}

//service implements IService
type service struct {
	name     string
	handlers map[string]IHandler
}

func (service service) Name() string {
	return service.name
}

func (service service) AddFunc(name string, handler HandlerFunc) {
	service.handlers[name] = handlerStruct{fnc: handler}
}

func (service service) AddStruct(name string, handler IHandler) {
	service.handlers[name] = handler
}

func (service service) Oper(name string) (IHandler, bool) {
	if f, ok := service.handlers[name]; ok {
		return f, true
	}
	return nil, false
}

func (service service) Opers() []string {
	names := []string{}
	for name := range service.handlers {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	return names
}

func (service service) Run(tmplRequest IRequest, tmplResponse IResponse) error {
	//determine configured rpc.server and run it
	serverConfigs := map[string]interface{}{}
	for n, c := range constructors {
		serverConfigs[n] = c
	}
	serverName, serverConfig, err := config.GetNamedStruct("rpc.server", serverConfigs)
	if err != nil {
		return fmt.Errorf("cannot get configured rpc.server: %v", err)
	}
	serverConstructor := serverConfig.(IServerConstructor)
	server, err := serverConstructor.Create(service, tmplRequest, tmplResponse)
	if err != nil {
		return fmt.Errorf("failed to create server(%s): %v", serverName, err)
	}
	return server.Run()
}

//handlerStruct is a wrapper around user functions when the user did not implement a request structure
type handlerStruct struct {
	fnc HandlerFunc
}

func (h handlerStruct) Exec(ctx IContext) (res interface{}, err error) {
	fmt.Printf("call h(%+v).fnc=%+v\n", h, h.fnc)
	return h.fnc(ctx, h)
}
