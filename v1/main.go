package main

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var logger *zap.Logger

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

	factory := informers.NewSharedInformerFactory(clientset, 10*time.Second)
	informer := factory.Core().V1().Pods().Informer()
	stopper := make(chan struct{})
	defer close(stopper)
	defer runtime.HandleCrash()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAdd,
		UpdateFunc: onUpdate,
		DeleteFunc: onDelete,
	})

	logger.Info("Running informer")
	go informer.Run(stopper)
	<-stopper
}

func onAdd(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		logger.With(zap.String("action", "create")).Error("Unserializable pod")
		return
	}
	logger.With(zap.String("action", "create")).Info(fmt.Sprintf("New Pod created %s/%s", pod.GetNamespace(), pod.GetName()))
}

func onUpdate(oldObj, newObj interface{}) {
	if oldObj == newObj {
		// new and old pod are the same. Nothing to do
		return
	}
	pod, ok := newObj.(*corev1.Pod)
	if !ok {
		logger.With(zap.String("action", "update")).Error("Unserializable pod")
		return
	}
	logger.With(zap.String("action", "update")).Info(fmt.Sprintf("New Pod created %s/%s", pod.GetNamespace(), pod.GetName()))
}

func onDelete(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		logger.With(zap.String("action", "delete")).Error("Unserializable pod")
		return
	}
	logger.With(zap.String("action", "delete")).Info(fmt.Sprintf("New Pod deleted %s/%s", pod.GetNamespace(), pod.GetName()))
}
