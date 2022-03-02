package etcdreg

import (
	"github.com/stretchr/testify/assert"
	"github.com/wuqtao/regdis"
	"sync"
	"testing"
	"time"
)

func Test_Naming(t *testing.T) {
	ns, err := NewEtcdRegistrationDiscoveryImp([]string{"127.0.0.1:2379"}, "", "", "/service/test")
	assert.Nil(t, err)

	// 准备工作
	serviceName := "service"
	ser1 := regdis.NewService("gateway", serviceName, "test1", "tcp", "127.0.0.1", 8099)
	ser2 := regdis.NewService("gateway", serviceName, "test2", "tcp", "127.0.0.1", 8099)
	_ = ns.Deregister(ser1)
	_ = ns.Deregister(ser2)
	time.Sleep(time.Second)

	// 1. 注册 test_1
	err = ns.Register(ser1)
	assert.Nil(t, err)
	time.Sleep(time.Second)
	// 2. 服务发现
	servs, err := ns.Find(serviceName)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(servs))
	assert.Equal(t, "test1", servs[0].ServiceID())
	t.Log(servs)
	time.Sleep(time.Second)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// 3. 监听服务实时变化（新增）
	_ = ns.Subscribe(serviceName, func(services []regdis.Service) {
		t.Log(len(services))

		assert.Equal(t, 2, len(services))
		//assert.Equal(t, "test2", services[1].ServiceID())
		wg.Done()
	})
	time.Sleep(time.Second)

	// 4. 注册 test_2 用于验证第3步
	err = ns.Register(ser2)
	assert.Nil(t, err)
	// 等 Watch 回调中的方法执行完成
	wg.Wait()
	time.Sleep(time.Second)

	_ = ns.Unsubscribe(serviceName)
	time.Sleep(time.Second)

	// 5. 服务发现
	servs, _ = ns.Find(serviceName)
	assert.Equal(t, 2, len(servs)) // <-- 必须有两个
	time.Sleep(time.Second)

	// 7. 注销test_2
	err = ns.Deregister(ser2)
	assert.Nil(t, err)
	time.Sleep(time.Second * 5)

	// 8. 服务发现
	servs, err = ns.Find(serviceName)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(servs))
	assert.Equal(t, "test1", servs[0].ServiceID())

	// 9. 注销test_1
	err = ns.Deregister(ser1)
	assert.Nil(t, err)
	time.Sleep(time.Second)
}
