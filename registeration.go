package regdis

type ServiceCallback func([]Service)

type RegistrationAndDiscovery interface {
	Register(ser Service) error   //向注册中心注册服务
	Deregister(ser Service) error //从注册中心取消注册

	Find(serviceName string) ([]Service, error)                   //根据服务名称从注册中心查找服务
	Subscribe(serviceName string, callback ServiceCallback) error //根据服务名订阅服务，订阅后有变动会回调
	Unsubscribe(serviceName string) error                         //取消订阅某类服务
}
