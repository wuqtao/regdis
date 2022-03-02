package regdis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Service interface {
	GetNamespace() string
	ServiceName() string
	ServiceID() string

	PublicAddress() string
	PublicPort() int
	DialURL() string
	GetProtocol() string
	String() string
}

type DefaultService struct {
	Name      string
	ID        string
	Namespace string
	Address   string
	Port      int
	Protocol  string
}

func (d *DefaultService) GetNamespace() string {
	return d.Namespace
}

func (d *DefaultService) ServiceName() string {
	return d.Name
}

func (d *DefaultService) ServiceID() string {
	return d.ID
}

func (d *DefaultService) PublicAddress() string {
	return d.Address
}

func (d *DefaultService) PublicPort() int {
	return d.Port
}

func (d *DefaultService) DialURL() string {
	if d.Protocol == "tcp" {
		return fmt.Sprintf("%s:%d", d.Address, d.Port)
	}
	return fmt.Sprintf("%s://%s:%d", d.Protocol, d.Address, d.Port)
}

func (d *DefaultService) GetProtocol() string {
	return d.Protocol
}

func (d *DefaultService) String() string {
	return fmt.Sprintf("Namespace:%s,ServiceName:%s,ServiceId:%s,Address:%s,Port:%d", d.Namespace, d.Name, d.ID, d.Address, d.Port)
}

func NewService(namespace, name, id, protocol, address string, port int) Service {
	return &DefaultService{
		Name:      name,
		ID:        id,
		Namespace: namespace,
		Address:   address,
		Port:      port,
		Protocol:  protocol,
	}
}

func ParseServiceFromStr(serviceStr string) (Service, error) {
	if serviceStr == "" || !strings.Contains(serviceStr, ":") || !strings.Contains(serviceStr, ",") {
		return nil, errors.New("serviceStr can not be empty")
	}

	strArr := strings.Split(serviceStr, ",")
	if len(strArr) < 5 {
		return nil, errors.New("serviceStr is less than 5 params and invalid for service")
	}
	service := &DefaultService{}
	for _, str := range strArr {
		arr := strings.Split(str, ":")
		if len(arr) != 2 {
			return nil, errors.New("service param is invalid")
		}
		switch arr[0] {
		case "Namespace":
			service.Namespace = arr[1]
		case "ServiceName":
			service.Name = arr[1]
		case "ServiceId":
			service.ID = arr[1]
		case "Address":
			service.Address = arr[1]
		case "Port":
			port, err := strconv.Atoi(arr[1])
			if err != nil {
				return nil, errors.New("port is not a valid num")
			}
			service.Port = port
		}
	}
	return service, nil
}
