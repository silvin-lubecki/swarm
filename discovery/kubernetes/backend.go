package kubernetes

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/docker/pkg/discovery"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// Discovery is exported.
type Discovery struct {
	heartbeat time.Duration
	ttl       time.Duration
	client    *kubernetes.Clientset
}

// Options to list only compatibility pods
var (
	options = metav1.ListOptions{LabelSelector: "app=compatibility"}
)

func init() {
	Init()
}

// Init is exported.
func Init() {
	log.Info("Registering kubernetes discovery backend...")
	discovery.Register("kubernetes", &Discovery{})
}

// Initialize is exported.
func (s *Discovery) Initialize(kubeconfig string, heartbeat time.Duration, ttl time.Duration, _ map[string]string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to initialize kubernetes discovery backend from config: %s", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client in discovery backend: %s", err)
	}

	s.client = client
	s.heartbeat = heartbeat
	s.ttl = ttl
	log.Info("Kubernetes discovery backend initialized.")
	return nil
}

// fetch returns the list of entries for the discovery service.
func (s *Discovery) fetch() (discovery.Entries, error) {
	log.Info("Kubernetes discovery backend fetching entries...")
	pods, err := s.client.CoreV1().Pods("docker").List(options)
	if err != nil {
		log.Error("Cannot list swarm classic pods", err)
		return nil, err
	}
	var addrs []string
	for _, pod := range pods.Items {
		log.Infof("POD: %s", pod.Name)
		if pod.Status.PodIP != "" {
			addrs = append(addrs, fmt.Sprintf("%s:2375", pod.Status.PodIP))
		}
	}
	log.Infof("Kubernetes discovery backend fetched addresses: %s", addrs)
	return discovery.CreateEntries(addrs)
}

// Watch is exported.
func (s *Discovery) Watch(stopCh <-chan struct{}) (<-chan discovery.Entries, <-chan error) {
	log.Info("Kubernetes discovery backend watching entries...")
	ch := make(chan discovery.Entries)
	ticker := time.NewTicker(s.heartbeat)
	errCh := make(chan error)

	var currentEntries discovery.Entries
	fetch := func() {
		newEntries, err := s.fetch()
		if err != nil {
			errCh <- err
		} else {
			// Check if the file has really changed.
			if !newEntries.Equals(currentEntries) {
				ch <- newEntries
			}
			currentEntries = newEntries
		}
	}

	go func() {
		defer close(ch)
		defer close(errCh)

		// Inialize pod event watcher
		watcher, err := s.client.CoreV1().Pods("docker").Watch(options)
		var podsCh <-chan watch.Event
		if err != nil {
			errCh <- fmt.Errorf("failed to watch compatibility pods: %s", err.Error())
		}
		if watcher != nil {
			podsCh = watcher.ResultChan()
			defer watcher.Stop()
		}

		// Send the initial entries if available.
		fetch()

		// Periodically send updates.
		for {
			select {
			case <-ticker.C:
				fetch()
			case event := <-podsCh:
				if event.Type != watch.Error {
					log.Infof("Pod event '%s': refetching entries", event.Type)
					fetch()
				}
			case <-stopCh:
				ticker.Stop()
				return
			}
		}
	}()
	log.Info("Kubernetes discovery backend watch ended.")
	return ch, errCh
}

// Register is exported
func (s *Discovery) Register(addr string) error {
	return discovery.ErrNotImplemented
}
