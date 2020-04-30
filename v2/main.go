package main

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

var logger *zap.Logger
var queue workqueue.RateLimitingInterface
var stopper chan struct{}

func main() {
	logger, _ = zap.NewProduction()
	defer logger.Sync()
	logger.Info("The k8s aws logger started")
	kubeconfig := os.Getenv("KUBECONFIG")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		logger.Panic(err.Error())
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Panic(err.Error())
		os.Exit(1)
	}
	logger.Info("Kubernetes targeted", zap.String("host", config.Host), zap.String("username", config.Username))
	queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer queue.ShutDown()
	go processQueue(stopper)

	factory := informers.NewSharedInformerFactory(clientset, 10*time.Second)
	informer := factory.Core().V1().Pods().Informer()
	stopper = make(chan struct{})
	defer close(stopper)
	defer runtime.HandleCrash()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			var key string
			var err error
			if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
				runtime.HandleError(err)
				return
			}
			queue.Add(key)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			var key string
			var err error
			if key, err = cache.MetaNamespaceKeyFunc(newObj); err != nil {
				runtime.HandleError(err)
				return
			}
			queue.Add(key)
		},
		DeleteFunc: func(obj interface{}) {
			var key string
			var err error
			if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
				runtime.HandleError(err)
				return
			}
			queue.Add(key)
		},
	})

	logger.Info("Running informer")
	go informer.Run(stopper)

	<-stopper
}

func processQueue(stopper chan struct{}) {
	for {
		// We use an anynomous function here to defer queue.Done function.
		// In this way we won't forget about the Done function that needs to be always called
		// otherwhise we won't notify the queue that a message got processed.
		func() {
			var key string
			var ok bool
			obj, shutdown := queue.Get()
			if shutdown {
				return
			}
			defer queue.Done(obj)
			if key, ok = obj.(string); !ok {
				queue.Forget(obj)
				runtime.HandleError(fmt.Errorf("key is not a string %#v", obj))
				return
			}
			namespace, name, err := cache.SplitMetaNamespaceKey(key)
			if err != nil {
				queue.Forget(key)
				runtime.HandleError(fmt.Errorf("Impossible to split key: %s", key))
				return
			}
			logger.With(zap.Int("queue_len", queue.Len())).With(zap.Int("num_retry", queue.NumRequeues(key))).Info(fmt.Sprintf("recevied key %s/%s", namespace, name))

			// you need to implement this function
			mockFunc := func() error { return nil }

			if err := mockFunc(); err != nil {
				// if it is a temporary erorr you can requeue the message
				queue.AddRateLimited(key)
				return
			}
			// Everything is done right. We can purge the message from the queue
			queue.Forget(key)
		}()
	}
}
