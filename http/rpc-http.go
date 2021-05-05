package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	nethttp "net/http"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/stewelarend/rpc"
)

func init() {
	rpc.RegisterServer(
		"http",
		&config{
			Address: "",
			Port:    8000,
		},
	)
}

type config struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

func (c *config) Validate() error {
	if c.Port == 0 {
		c.Port = 8000
	}
	if c.Port < 0 {
		return fmt.Errorf("negative port:%d", c.Port)
	}
	return nil
}

func (c config) Create(service rpc.IService) (rpc.IServer, error) {
	fmt.Printf("Create HTTP server: %+v\n", c)
	return httpRpcServer{
		service: service,
		config:  c,
	}, nil
}

type httpRpcServer struct {
	service rpc.IService
	config  config
}

func (s httpRpcServer) Run() error {
	return nethttp.ListenAndServe(fmt.Sprintf("%s:%d", s.config.Address, s.config.Port), s)
}

func (s httpRpcServer) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	fmt.Printf("HTTP %s %s\n", req.Method, req.URL.Path)
	if req.Method != http.MethodGet && req.Method != http.MethodPost {
		http.Error(res, "method must be GET or POST", http.StatusMethodNotAllowed)
		return
	}

	operName := path.Clean(req.URL.Path)[1:] //skip leading '/'
	operHandler, ok := s.service.Oper(operName)
	if !ok {
		http.Error(
			res,
			fmt.Sprintf("operation \"%s\" does not exist, only %s",
				operName,
				strings.Join(s.service.Opers(), "|")),
			http.StatusNotFound)
		return
	}

	//create a new handler struct instance and copy the value from the registered struct
	//(it copies defaults defined by the user and fnc in rpc.go's handlerStruct...)
	handlerType := reflect.TypeOf(operHandler)
	if handlerType.Kind() == reflect.Ptr {
		handlerType = handlerType.Elem()
	}
	handlerStructPtrValue := reflect.New(handlerType)
	handlerStructPtrValue.Elem().Set(reflect.ValueOf(operHandler))
	//now use this oper handler going forward instead of registered one
	operHandler, ok = handlerStructPtrValue.Interface().(rpc.IHandler)
	if !ok {
		http.Error(
			res,
			fmt.Sprintf(
				"cannot convert %T to handler",
				handlerStructPtrValue.Interface()),
			http.StatusInternalServerError)
		return
	}

	if req.Method == http.MethodPost {
		//HTTP POST: parse the body into the req struct
		if err := json.NewDecoder(req.Body).Decode(handlerStructPtrValue.Interface()); err != nil {
			http.Error(
				res,
				fmt.Sprintf(
					"cannot parse body into %T",
					handlerStructPtrValue.Interface()),
				http.StatusBadRequest)
			return
		}
	}
	//parse URL params into the struct
	if err := parseIntoStruct(handlerStructPtrValue, urlParams(req.URL.Query())); err != nil {
		http.Error(
			res,
			fmt.Sprintf(
				"cannot parse url params into %T: %v",
				handlerStructPtrValue.Interface(),
				err),
			http.StatusBadRequest)
		return
	}

	if validator, ok := handlerStructPtrValue.Interface().(rpc.IValidator); ok {
		if err := validator.Validate(); err != nil {
			http.Error(
				res,
				fmt.Sprintf(
					"invalid request: %v",
					err),
				http.StatusBadRequest)
			return
		}
	}

	operRes, err := operHandler.Exec(nil)
	if err != nil {
		http.Error(
			res,
			fmt.Sprintf("operation \"%s\" failed: %v",
				operName,
				err),
			http.StatusBadRequest) //todo - wrong error!
		return
	}
	jsonRes, err := json.Marshal(operRes)
	if err != nil {
		http.Error(
			res,
			fmt.Sprintf("operation \"%s\" response encoding failed: %v",
				operName,
				err),
			http.StatusBadRequest) //todo - wrong error!
		return
	}

	res.Header().Set("Content-Type", "application/json")
	res.Write(jsonRes)
}

func urlParams(p map[string][]string) map[string]interface{} {
	paramsObject := map[string]interface{}{}
	for n, v := range p {
		if len(v) > 1 {
			paramsObject[n] = v
		} else {
			paramsObject[n] = v[0]
		}
	}
	return paramsObject
}

func parseIntoStruct(structPtrValue reflect.Value, params map[string]interface{}) error {
	if structPtrValue.Kind() != reflect.Ptr ||
		structPtrValue.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("%v is not struct ptr value", structPtrValue)
	}

	structType := structPtrValue.Type().Elem()
	for paramName, paramValue := range params {
		structField, ok := structType.FieldByNameFunc(func(fieldName string) bool {
			if paramName == fieldName {
				return true
			}
			structTypeField, _ := structType.FieldByName(fieldName)
			jsonTags := strings.SplitN(structTypeField.Tag.Get("json"), ",", 2)
			if len(jsonTags) > 0 && jsonTags[0] == paramName {
				return true
			}
			return false
		})
		if !ok {
			return fmt.Errorf("%s has no field \"%s\"", structType.Name(), paramName)
		}

		//parse the value into the field
		fieldValue := structPtrValue.Elem().FieldByIndex(structField.Index)
		switch fieldValue.Type().Kind() {
		case reflect.String:
			fieldValue.Set(reflect.ValueOf(fmt.Sprintf("%v", paramValue)))
		case reflect.Int:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 64)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(int(intValue)))
		case reflect.Int8:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(int8(intValue)))
		case reflect.Uint8:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(uint8(intValue)))
		case reflect.Int16:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(int16(intValue)))
		case reflect.Uint16:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(uint16(intValue)))
		case reflect.Int32:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(int32(intValue)))
		case reflect.Uint32:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(uint32(intValue)))
		case reflect.Int64:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(int64(intValue)))
		case reflect.Uint64:
			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", paramValue), 10, 8)
			if err != nil {
				return fmt.Errorf("cannot parse int from %s=(%T)%v for %v", paramName, paramValue, paramValue, fieldValue.Type())
			}
			fieldValue.Set(reflect.ValueOf(uint64(intValue)))
		default:
			return fmt.Errorf("cannot store %s=(%T)%v in %v", paramName, paramValue, paramValue, fieldValue.Type())
		}
	}
	return nil
}
